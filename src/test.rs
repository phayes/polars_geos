use super::*;

#[test]
fn test_buffer() {
    let geojson = r#"{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"name":"foo"},"geometry":{"type":"Point","coordinates":[1,2]}},{"type":"Feature","properties":{"name":"bar"},"geometry":{"type":"Point","coordinates":[3,4]}}]}"#.as_bytes();
    let mut df = polars_gdal::df_from_bytes(geojson, None, None).unwrap();

    let series = df.column("geometry").unwrap();
    let buffered = series.geos_buffer(1.0, 8).unwrap();
    let df = df.with_column(buffered).unwrap();

    println!("{}", df);
}

#[test]
fn test_intersection() {
    let mut lakes = polars_gdal::df_from_resource("./testdata/global_large_lakes.feature_collection.implicit_4326.json", None).unwrap();
    let states = polars_gdal::df_from_resource("./testdata/us_states.feature_collection.implicit_4326.json", None).unwrap();

    let all_states_geom = states.column("geometry").unwrap();
    let all_states_uniary_geom = all_states_geom.geos_self_union().unwrap();

    let lakes_geom = lakes.column("geometry").unwrap();
    let lakes_geom_intersected = lakes_geom.geos_geom_intersection(&all_states_uniary_geom).unwrap();

    let df = lakes.with_column(lakes_geom_intersected).unwrap();

    println!("{}", df);
}