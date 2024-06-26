extern crate pg_interval;
extern crate postgres_types;
extern crate serde;

use std::error::Error;
use std::str::FromStr;
use postgres_types::{accepts, FromSql, IsNull, to_sql_checked, ToSql, Type};
use postgres_types::private::BytesMut;
use serde::{Deserialize, Serialize};
use serde::ser::{SerializeStruct};
use std::convert::TryInto;
use std::fmt::{Display, Formatter, Write};

#[derive(Debug, PartialEq, Eq)]
pub struct ParseError {
    pg: pg_interval::ParseError,
}

impl From<pg_interval::ParseError> for ParseError {
    fn from(pg: pg_interval::ParseError) -> ParseError {
        ParseError {
            pg
        }
    }
}

impl Into<pg_interval::ParseError> for ParseError {
    fn into(self) -> pg_interval::ParseError {
        self.pg
    }
}

impl Display for ParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.pg {
            pg_interval::ParseError::InvalidInterval(s) => write!(f, "{}", s),
            pg_interval::ParseError::InvalidTime(s) => write!(f, "{}", s),
            pg_interval::ParseError::InvalidYearMonth(s) => write!(f, "{}", s),
            pg_interval::ParseError::ParseIntErr(s) => write!(f, "{}", s),
            pg_interval::ParseError::ParseFloatErr(s) => write!(f, "{}", s),
        }
    }
}

impl Error for ParseError {
}

#[derive(Debug)]
pub struct Interval {
    pg: pg_interval::Interval,
}

impl Interval {
    pub fn new(interval: &str) -> Result<Interval, ParseError> {
        Ok(Interval {
            pg: pg_interval::Interval::from_postgres(&interval)?,
        })
    }

    pub fn inner(&self) -> &pg_interval::Interval {
        &self.pg
    }

    pub fn bytes(&self) -> Vec<u8> {
        let mut buf = vec![0u8, 16];
        buf[0..8].copy_from_slice(&self.pg.microseconds.to_be_bytes());
        buf[8..12].copy_from_slice(&self.pg.days.to_be_bytes());
        buf[12..16].copy_from_slice(&self.pg.months.to_be_bytes());
        buf
    }
}

impl FromStr for Interval {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Interval::new(s)
    }
}

impl Display for Interval {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let years = self.pg.months / 12;
        let months = self.pg.months % 12;
        let days = self.pg.days;
        let hours = self.pg.microseconds / 3_600_000_000;
        let minutes = (self.pg.microseconds % 3_600_000_000) / 60_000_000;
        let seconds = (self.pg.microseconds % 60_000_000) / 1_000_000;
        let milliseconds = self.pg.microseconds % 1_000_000 / 1_000;
        let microseconds = self.pg.microseconds % 1_000;
        let mut buf = String::new();
        if years > 0 {
            write!(buf, "{} years ", years)?;
        }
        if months > 0 {
            write!(buf, "{} mons ", months)?;
        }
        if days > 0 {
            write!(buf, "{} days ", days)?;
        }
        if hours > 0 {
            write!(buf, "{} hours ", hours)?;
        }
        if minutes > 0 {
            write!(buf, "{} minutes ", minutes)?;
        }
        if seconds > 0 {
            write!(buf, "{} seconds ", seconds)?;
        }
        if milliseconds > 0 {
            write!(buf, "{} milliseconds ", milliseconds)?;
        }
        if microseconds > 0 {
            write!(buf, "{} microseconds ", microseconds)?;
        }
        if buf.is_empty() {
            write!(buf, "0 seconds")
        } else {
            write!(f, "{}", &buf.as_str()[..buf.len() - 1])
        }
    }
}

impl<'a> FromSql<'a> for Interval {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Interval, Box<dyn Error + Sync + Send>> {
        Ok(Interval {
            pg: pg_interval::Interval {
                months: i32::from_be_bytes(raw[12..16].try_into().unwrap()),
                days: i32::from_be_bytes(raw[8..12].try_into().unwrap()),
                microseconds: i64::from_be_bytes(raw[0..8].try_into().unwrap()),
            }
        })
    }

    accepts!(INTERVAL);
}

impl ToSql for Interval {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        w.extend_from_slice(self.pg.microseconds.to_be_bytes().as_slice());
        w.extend_from_slice(self.pg.days.to_be_bytes().as_slice());
        w.extend_from_slice(self.pg.months.to_be_bytes().as_slice());
        Ok(IsNull::No)
    }

    accepts!(INTERVAL);
    to_sql_checked!();
}

impl Serialize for Interval {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("Interval", 3)?;
        state.serialize_field("m", &self.pg.months)?;
        state.serialize_field("d", &self.pg.days)?;
        state.serialize_field("us", &self.pg.microseconds)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for Interval {
    fn deserialize<D>(deserializer: D) -> Result<Interval, D::Error>
        where
            D: serde::Deserializer<'de>,
    {
        struct IntervalVisitor;

        impl<'de> serde::de::Visitor<'de> for IntervalVisitor {
            type Value = Interval;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a string representing an interval")
            }

            fn visit_map<V>(self, mut visitor: V) -> Result<Interval, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut months = None;
                let mut days = None;
                let mut microseconds = None;

                while let Some(key) = visitor.next_key()? {
                    match key {
                        "m" => {
                            if months.is_some() {
                                return Err(serde::de::Error::duplicate_field("m"));
                            }
                            months = Some(visitor.next_value()?);
                        }
                        "d" => {
                            if days.is_some() {
                                return Err(serde::de::Error::duplicate_field("d"));
                            }
                            days = Some(visitor.next_value()?);
                        }
                        "us" => {
                            if microseconds.is_some() {
                                return Err(serde::de::Error::duplicate_field("us"));
                            }
                            microseconds = Some(visitor.next_value()?);
                        }
                        _ => {
                            return Err(serde::de::Error::unknown_field(key, &["m", "d", "us"]));
                        }
                    }
                }

                let months = months.ok_or_else(|| serde::de::Error::missing_field("m"))?;
                let days = days.ok_or_else(|| serde::de::Error::missing_field("d"))?;
                let microseconds = microseconds.ok_or_else(|| serde::de::Error::missing_field("us"))?;

                Ok(Interval {
                    pg: pg_interval::Interval {
                        months,
                        days,
                        microseconds,
                    }
                })
            }
        }

        deserializer.deserialize_struct("Interval", &["m", "d", "us"], IntervalVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interval_from_str() {
        let interval = Interval::new("1 mons 2 days 3 seconds").unwrap();
        assert_eq!(interval.pg.months, 1);
        assert_eq!(interval.pg.days, 2);
        assert_eq!(interval.pg.microseconds, 3000000);
    }

    #[test]
    fn test_interval_from_sql() {
        let interval = Interval::from_sql(&Type::INTERVAL, &[0, 0, 0, 0, 0, 45, 198, 192, 0, 0, 0, 2, 0, 0, 0, 1]).unwrap();
        assert_eq!(interval.pg.months, 1);
        assert_eq!(interval.pg.days, 2);
        assert_eq!(interval.pg.microseconds, 3000000);
    }

    #[test]
    fn test_interval_to_sql() {
        let interval = Interval {
            pg: pg_interval::Interval {
                months: 1,
                days: 2,
                microseconds: 3000000,
            }
        };
        let mut buf = BytesMut::new();
        interval.to_sql(&Type::INTERVAL, &mut buf).unwrap();
        assert_eq!(buf.as_ref(), &[0, 0, 0, 0, 0, 45, 198, 192, 0, 0, 0, 2, 0, 0, 0, 1]);
    }

    #[test]
    fn test_interval_display() {
        let interval = Interval {
            pg: pg_interval::Interval {
                months: 14,
                days: 3,
                microseconds: 4 * 3600000000 + 5 * 60000000 + 6 * 1000000 + 7 * 1000 + 8,
            }
        };
        assert_eq!(format!("{}", interval), "1 years 2 mons 3 days 4 hours 5 minutes 6 seconds 7 milliseconds 8 microseconds");

        let interval = Interval {
            pg: pg_interval::Interval {
                months: 1,
                days: 2,
                microseconds: 3 * 1000000,
            }
        };
        assert_eq!(interval.to_string(), "1 mons 2 days 3 seconds");
    }

    #[test]
    fn test_interval_serialize() {
        let interval = Interval {
            pg: pg_interval::Interval {
                months: 1,
                days: 2,
                microseconds: 3,
            }
        };
        let serialized = serde_json::to_string(&interval).unwrap();
        assert_eq!(serialized, r#"{"m":1,"d":2,"us":3}"#);
    }

    #[test]
    fn test_interval_deserialize() {
        let deserialized: Interval = serde_json::from_str(r#"{"m":1,"d":2,"us":3}"#).unwrap();
        assert_eq!(deserialized.pg.months, 1);
        assert_eq!(deserialized.pg.days, 2);
        assert_eq!(deserialized.pg.microseconds, 3);
    }

    #[test]
    fn test_anyhow_error_propagation() {
        let interval = (|| -> anyhow::Result<Interval> {
            Ok(Interval::new("1 monthss")?)
        })();
        assert_eq!(interval.err().unwrap().to_string(), "Unknown or duplicate deliminator \"monthss\"");
    }
}
