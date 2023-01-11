mod error;

#[cfg(test)]
mod test;

pub use geos;

use error::*;
use geos::Geom;
use geos::Geometry;
use polars::export::arrow::array::{Array, BinaryArray, BooleanArray, MutableBinaryArray, MutableBooleanArray};
use polars::prelude::Series;
use std::convert::Into;

pub type ArrayRef = Box<dyn Array>;

pub trait GeosSeries {
    /// For each geometry in the series, check to see if it's valid. Returns a Boolean Series
    fn is_valid(&self) -> Result<Series>;

    /// For each geometry in the series, try to make it valid
    fn make_valid(&self) -> Result<Series>;

    /// Returns a polygon or multipolygon geometry series which represents all points whose distance from self is less than or equal to distance. Read more
    fn buffer(&self, width: f64, quadsegs: i32) -> Result<Series>;

    /// Returns a geometry series representing the intersection between self and another series.
    fn intersection(&self, other: &Series) -> Result<Series>;

    /// Returns a geometry series representing the intersection between self and another geometry.
    fn geom_intersection(&self, other: &geos::Geometry) -> Result<Series>;

    /// Returns a geometry series representing the difference between self and another series. Difference represents part of self that doesn’t intersect with other.
    fn difference(&self, other: &Series) -> Result<Series>;

    /// Returns a geometry series representing the difference between self and another geometry. Difference represents part of self that doesn’t intersect with other.
    fn geom_difference(&self, other: &geos::Geometry) -> Result<Series>;

    /// Union all geometries in a series to produce a single geometry. Does not dissolve boundaries.
    fn self_union(&self) -> Result<geos::Geometry>;
}

impl GeosSeries for Series {
    fn is_valid(&self) -> Result<Series> {
        let mut output_array = MutableBooleanArray::with_capacity(self.len());
        for geom in iter_geom(self) {
            match geom {
                Some(geom) => {
                    let valid = geom.is_valid();
                    output_array.push(Some(valid));
                }
                None => {
                    output_array.push(None);
                }
            }
        }

        let result: BooleanArray = output_array.into();

        let series = Series::try_from(("is_valid", Box::new(result) as ArrayRef))?;
        Ok(series)
    }

    fn make_valid(&self) -> Result<Series> {
        let mut output_array = MutableBinaryArray::<i32>::with_capacity(self.len());
        for geom in iter_geom(self) {
            match geom {
                Some(geom) => {
                    let value = geom.make_valid()?;
                    let wkb = value.to_wkb()?;
                    output_array.push(Some(wkb));
                }
                None => {
                    output_array.push::<&[u8]>(None);
                }
            }
        }

        let result: BinaryArray<i32> = output_array.into();

        let series = Series::try_from(("geometry", Box::new(result) as ArrayRef))?;
        Ok(series)
    }

    fn buffer(&self, width: f64, quadsegs: i32) -> Result<Series> {
        let mut output_array = MutableBinaryArray::<i32>::with_capacity(self.len());
        for geom in iter_geom(self) {
            match geom {
                Some(geom) => {
                    let value = geom.buffer(width, quadsegs)?;
                    let wkb = value.to_wkb()?;
                    output_array.push(Some(wkb));
                }
                None => {
                    output_array.push::<&[u8]>(None);
                }
            }
        }

        let result: BinaryArray<i32> = output_array.into();

        let series = Series::try_from(("geometry", Box::new(result) as ArrayRef))?;
        Ok(series)
    }

    fn self_union(&self) -> Result<geos::Geometry> {
        let base_geom = iter_geom(self).enumerate().find(|geom| geom.1.is_some());
        let base_geom = base_geom.ok_or(PolarsGeosError::NoGeometries)?;

        match base_geom.1 {
            Some(init) => {
                let unioned = iter_geom(self)
                    .skip(base_geom.0 + 1)
                    .fold(init, |acc, geom| match geom {
                        Some(geos_geom) => {
                            acc.union(&geos_geom).unwrap()
                        }
                        None => acc,
                    });
                Ok(unioned)
            }
            None => Err(PolarsGeosError::NoGeometries),
        }
    }

    fn geom_intersection(&self, other: &geos::Geometry) -> Result<Series> {
        let other_prepared = other.to_prepared_geom()?;

        let mut output_array = MutableBinaryArray::<i32>::with_capacity(self.len());
        for geom in iter_geom(self) {
            match geom {
                Some(geom) => {
                    let value = if other_prepared.intersects(&geom)? {
                        let intersected = geom.intersection(other)?;
                        if intersected.is_empty()? {
                            None
                        } else {
                            Some(intersected)
                        }
                    } else {
                        None
                    };
        
                    match value {
                        Some(value) => {
                            let wkb = value.to_wkb()?;
                            output_array.push(Some(wkb));
                        }
                        None => {
                            output_array.push::<&[u8]>(None);
                        }
                    };
                },
                None => {
                    output_array.push::<&[u8]>(None);
                }
            }
        }

        let result: BinaryArray<i32> = output_array.into();

        let series = Series::try_from(("geometry", Box::new(result) as ArrayRef))?;
        Ok(series)
    }


    fn intersection(&self, other: &Series) -> Result<Series> {
        let mut output_array = MutableBinaryArray::<i32>::with_capacity(self.len());
        for geoms in iter_geom(self).zip(iter_geom(other)) {
            // Combine the two geometries into a tuple
            let geoms = geoms.0.and_then(|a| geoms.1.map(|b| (a, b)));

            match geoms {
                Some(geoms) => {
                    let intersected = geoms.0.intersection(&geoms.1)?;
                    if intersected.is_empty()? {
                        output_array.push::<&[u8]>(None);
                    }
                    else {
                        let wkb = intersected.to_wkb()?;
                        output_array.push(Some(wkb));
                    }
                },
                None => {
                    output_array.push::<&[u8]>(None);
                }
            }
        }

        let result: BinaryArray<i32> = output_array.into();

        let series = Series::try_from(("geometry", Box::new(result) as ArrayRef))?;
        Ok(series)
    }

    fn geom_difference(&self, other: &geos::Geometry) -> Result<Series> {
        let other_prepared = other.to_prepared_geom()?;

        let mut output_array = MutableBinaryArray::<i32>::with_capacity(self.len());
        for geom in iter_geom(self) {
            match geom {
                Some(geom) => {
                    let value = if other_prepared.intersects(&geom)? {
                        geom.difference(other)?
                    } else {
                       geom
                    };
                    let wkb = value.to_wkb()?;
                    output_array.push(Some(wkb));
                },
                None => {
                    output_array.push::<&[u8]>(None);
                }
            }
        }

        let result: BinaryArray<i32> = output_array.into();

        let series = Series::try_from(("geometry", Box::new(result) as ArrayRef))?;
        Ok(series)
    }


    fn difference(&self, other: &Series) -> Result<Series> {
        let mut output_array = MutableBinaryArray::<i32>::with_capacity(self.len());
        for geoms in iter_geom(self).zip(iter_geom(other)) {
            match (geoms.0, geoms.1) {
                (Some(self_geom), Some(other_geom)) => {
                    let difference = self_geom.difference(&other_geom)?;
                    let wkb = difference.to_wkb()?;
                    output_array.push(Some(wkb));
                },
                (Some(self_geom), None) => {
                    let wkb = self_geom.to_wkb()?;
                    output_array.push(Some(wkb));
                },
                (None, _) => {
                    output_array.push::<&[u8]>(None);
                }
            }
        }

        let result: BinaryArray<i32> = output_array.into();

        let series = Series::try_from(("geometry", Box::new(result) as ArrayRef))?;
        Ok(series)
    }
}

/// Helper function to iterate over geometries from polars Series
pub(crate) fn iter_geom(series: &Series) -> impl Iterator<Item = Option<Geometry<'_>>> {
    let chunks = series.binary().expect("series was not a list type");

    let iter = chunks.into_iter();
    iter.map(|row| match row {
        Some(value) => {
            Some(Geometry::new_from_wkb(value).expect("unable to convert to geos geometry"))
        }
        None => None,
    })
}
