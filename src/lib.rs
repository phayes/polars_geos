mod error;

#[cfg(test)]
mod test;

pub use geos;

use error::*;
use polars::export::arrow::array::{
    Array, BinaryArray, MutableBinaryArray
};
use polars::prelude::Series;
use std::convert::Into;
use geos::Geometry;
use geopolars::geoseries::GeoSeries;
use geos::Geom;

pub type ArrayRef = Box<dyn Array>;

pub trait GeosGeoSeries {
    /// Returns a polygon or multipolygon geometry series which represents all points whose distance from self is less than or equal to distance. Read more
    fn buffer(&self, width: f64, quadsegs: i32) -> Result<Series>;

    /// Returns a geometry series representing the intersection between self and other. Read more
    fn intersection(&self, other: &geos::Geometry) -> Result<Series>;
}

impl GeosGeoSeries for Series where Series: GeoSeries {
    fn buffer(&self, width: f64, quadsegs: i32) -> Result<Series> {
        let mut output_array = MutableBinaryArray::<i32>::with_capacity(self.len());    
        for geom in iter_geom(self) {
            let value = geom.buffer(width, quadsegs)?;
            let wkb = value.to_wkb()?;
            output_array.push(Some(wkb));
        }
    
        let result: BinaryArray<i32> = output_array.into();
    
        let series = Series::try_from(("geometry", Box::new(result) as ArrayRef))?;
        Ok(series)
    }

    fn intersection(&self, other: &geos::Geometry) -> Result<Series> {
        let other_prepared = other.to_prepared_geom()?;

        let mut output_array = MutableBinaryArray::<i32>::with_capacity(self.len());    
        for geom in iter_geom(self) {
            let value = if other_prepared.intersects(&geom)? {
                Some(geom.intersection(other)?)
            } else {
                None
            };
            let wkb = value.to_wkb()?;
            output_array.push(Some(wkb));
        }
    
        let result: BinaryArray<i32> = output_array.into();
    
        let series = Series::try_from(("geometry", Box::new(result) as ArrayRef))?;
        Ok(series)
    }
}

/// Helper function to iterate over geometries from polars Series
pub(crate) fn iter_geom(series: &Series) -> impl Iterator<Item = Geometry<'_>> {
    let chunks = series.binary().expect("series was not a list type");

    let iter = chunks.into_iter();
    iter.map(|row| {
        let value = row.expect("Row is null");
        Geometry::new_from_wkb(value).expect("unable to convert to geos geometry")
    })
}
