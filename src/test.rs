use super::*;

#[test]
fn test_buffer() {
    let geojson = r#"{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"name":"foo"},"geometry":{"type":"Point","coordinates":[1,2]}},{"type":"Feature","properties":{"name":"bar"},"geometry":{"type":"Point","coordinates":[3,4]}}]}"#.as_bytes();
    let mut df = polars_gdal::df_from_bytes(geojson, None, None).unwrap();

    let series = df.column("geometry").unwrap();
    let buffered = series.buffer(1.0, 8).unwrap();
    let df = df.with_column(buffered).unwrap();

    println!("{}", df);
}