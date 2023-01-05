use polars::error::PolarsError;
use geopolars::error::GeopolarsError;
use geos::Error as GeosError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum GeopolarsGeosError {
    // Copied from geo-types:
    // https://github.com/georust/geo/blob/a1226940a674c7ac5d1db43d495520e418af8907/geo-types/src/error.rs
    #[error("Expected {expected} (found {found})")]
    MismatchedGeometry {
        expected: &'static str,
        found: &'static str,
    },

    #[error(transparent)]
    PolarsError(Box<PolarsError>),

    #[error(transparent)]
    GeopolarsError(Box<GeopolarsError>),

    #[error(transparent)]
    GeosError(Box<GeosError>),
}

pub type Result<T> = std::result::Result<T, GeopolarsGeosError>;

impl From<PolarsError> for GeopolarsGeosError {
    fn from(err: PolarsError) -> Self {
        Self::PolarsError(Box::new(err))
    }
}

impl From<GeopolarsError> for GeopolarsGeosError {
    fn from(err: GeopolarsError) -> Self {
        Self::GeopolarsError(Box::new(err))
    }
}

impl From<GeosError> for GeopolarsGeosError {
    fn from(err: GeosError) -> Self {
        Self::GeosError(Box::new(err))
    }
}