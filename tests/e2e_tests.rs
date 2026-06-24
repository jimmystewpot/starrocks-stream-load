#![allow(
    clippy::pedantic,
    clippy::uninlined_format_args,
    clippy::manual_assert,
    clippy::print_stdout,
    clippy::needless_borrows_for_generic_args
)]

use bytes::Bytes;
use starrocks_stream_load::{
    DataFormat, StreamLoadConfig, StreamLoadManager, StreamLoadTableProperties,
};
use std::env;
use std::process::Command;
use std::time::Duration;

fn is_e2e_enabled() -> bool {
    env::var("STARROCKS_E2E").is_ok()
}

fn ensure_decompressed(filename: &str) {
    let uncompressed_path = format!("test_data/{}", filename);
    if std::path::Path::new(&uncompressed_path).exists() {
        return;
    }

    let compressed_path = format!("test_data/{}.gz", filename);
    println!("Decompressing {}...", compressed_path);
    let output = Command::new("gunzip")
        .args(["-k", "-f", &compressed_path])
        .output()
        .expect("Failed to execute gunzip command");

    assert!(
        output.status.success(),
        "Failed to decompress file {}: {}",
        compressed_path,
        String::from_utf8_lossy(&output.stderr)
    );
}

fn truncate_table(table: &str) {
    let mut attempts = 0;
    loop {
        attempts += 1;
        let output = Command::new("docker")
            .args(&[
                "exec",
                "quickstart",
                "mysql",
                "-P",
                "9030",
                "-h",
                "127.0.0.1",
                "-u",
                "root",
                "-e",
                &format!("TRUNCATE TABLE quickstart.{};", table),
            ])
            .output()
            .expect("Failed to execute truncate command");

        if output.status.success() {
            break;
        }

        let err_msg = String::from_utf8_lossy(&output.stderr);
        if attempts >= 5 {
            panic!(
                "Failed to truncate table {} after 5 attempts: {}",
                table, err_msg
            );
        }

        println!(
            "Truncate failed (attempt {}): {}. Retrying in 2 seconds...",
            attempts,
            err_msg.trim()
        );
        std::thread::sleep(Duration::from_secs(2));
    }
}

fn get_table_count(table: &str) -> usize {
    let output = Command::new("docker")
        .args(&[
            "exec",
            "quickstart",
            "mysql",
            "-P",
            "9030",
            "-h",
            "127.0.0.1",
            "-u",
            "root",
            "-N",
            "-e",
            &format!("SELECT COUNT(*) FROM quickstart.{};", table),
        ])
        .output()
        .expect("Failed to execute SELECT COUNT(*) command");

    if !output.status.success() {
        panic!(
            "Failed to get row count for table {}: {}",
            table,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout.trim().parse().unwrap_or(0)
}

fn get_config() -> StreamLoadConfig {
    StreamLoadConfig::builder(
        vec!["http://127.0.0.1:8030".to_string()],
        "quickstart".to_string(),
        "root".to_string(),
    )
    .password("")
    .connect_timeout(Duration::from_secs(15))
    .request_timeout(Duration::from_secs(600))
    .build()
}

#[tokio::test]
async fn test_e2e_csv_weatherdata() {
    if !is_e2e_enabled() {
        println!("Skipping E2E test: STARROCKS_E2E is not set.");
        return;
    }

    truncate_table("weatherdata");

    ensure_decompressed("72505394728.csv");
    let config = get_config();
    let props = StreamLoadTableProperties::builder()
        .table("weatherdata")
        .format(DataFormat::CSV)
        .column_separator(",")
        .skip_header(1)
        .header("enclose", "\"")
        .columns("STATION, DATE, LATITUDE, LONGITUDE, ELEVATION, NAME, REPORT_TYPE, SOURCE, HourlyAltimeterSetting, HourlyDewPointTemperature, HourlyDryBulbTemperature, HourlyPrecipitation, HourlyPresentWeatherType, HourlyPressureChange, HourlyPressureTendency, HourlyRelativeHumidity, HourlySkyConditions, HourlySeaLevelPressure, HourlyStationPressure, HourlyVisibility, HourlyWetBulbTemperature, HourlyWindDirection, HourlyWindGustSpeed, HourlyWindSpeed, Sunrise, Sunset, DailyAverageDewPointTemperature, DailyAverageDryBulbTemperature, DailyAverageRelativeHumidity, DailyAverageSeaLevelPressure, DailyAverageStationPressure, DailyAverageWetBulbTemperature, DailyAverageWindSpeed, DailyCoolingDegreeDays, DailyDepartureFromNormalAverageTemperature, DailyHeatingDegreeDays, DailyMaximumDryBulbTemperature, DailyMinimumDryBulbTemperature, DailyPeakWindDirection, DailyPeakWindSpeed, DailyPrecipitation, DailySnowDepth, DailySnowfall, DailySustainedWindDirection, DailySustainedWindSpeed, DailyWeather, MonthlyAverageRH, MonthlyDaysWithGT001Precip, MonthlyDaysWithGT010Precip, MonthlyDaysWithGT32Temp, MonthlyDaysWithGT90Temp, MonthlyDaysWithLT0Temp, MonthlyDaysWithLT32Temp, MonthlyDepartureFromNormalAverageTemperature, MonthlyDepartureFromNormalCoolingDegreeDays, MonthlyDepartureFromNormalHeatingDegreeDays, MonthlyDepartureFromNormalMaximumTemperature, MonthlyDepartureFromNormalMinimumTemperature, MonthlyDepartureFromNormalPrecipitation, MonthlyDewpointTemperature, MonthlyGreatestPrecip, MonthlyGreatestPrecipDate, MonthlyGreatestSnowDepth, MonthlyGreatestSnowDepthDate, MonthlyGreatestSnowfall, MonthlyGreatestSnowfallDate, MonthlyMaxSeaLevelPressureValue, MonthlyMaxSeaLevelPressureValueDate, MonthlyMaxSeaLevelPressureValueTime, MonthlyMaximumTemperature, MonthlyMeanTemperature, MonthlyMinSeaLevelPressureValue, MonthlyMinSeaLevelPressureValueDate, MonthlyMinSeaLevelPressureValueTime, MonthlyMinimumTemperature, MonthlySeaLevelPressure, MonthlyStationPressure, MonthlyTotalLiquidPrecipitation, MonthlyTotalSnowfall, MonthlyWetBulb, AWND, CDSD, CLDD, DSNW, HDSD, HTDD, NormalsCoolingDegreeDay, NormalsHeatingDegreeDay, ShortDurationEndDate005, ShortDurationEndDate010, ShortDurationEndDate015, ShortDurationEndDate020, ShortDurationEndDate030, ShortDurationEndDate045, ShortDurationEndDate060, ShortDurationEndDate080, ShortDurationEndDate100, ShortDurationEndDate120, ShortDurationEndDate150, ShortDurationEndDate180, ShortDurationPrecipitationValue005, ShortDurationPrecipitationValue010, ShortDurationPrecipitationValue015, ShortDurationPrecipitationValue020, ShortDurationPrecipitationValue030, ShortDurationPrecipitationValue045, ShortDurationPrecipitationValue060, ShortDurationPrecipitationValue080, ShortDurationPrecipitationValue100, ShortDurationPrecipitationValue120, ShortDurationPrecipitationValue150, ShortDurationPrecipitationValue180, REM, BackupDirection, BackupDistance, BackupDistanceUnit, BackupElements, BackupElevation, BackupEquipment, BackupLatitude, BackupLongitude, BackupName, WindEquipmentChangeDate")
        .build();

    let manager = StreamLoadManager::new(config, props).expect("Failed to create manager");

    let data_bytes = std::fs::read("test_data/72505394728.csv")
        .map(Bytes::from)
        .expect("Failed to read test dataset file");

    println!("Loading weatherdata CSV ({} bytes)...", data_bytes.len());
    let response = manager
        .send_single_batch("weather-csv-e2e", data_bytes)
        .await
        .expect("Stream load failed");

    assert_eq!(response.status, "Success");
    assert_eq!(response.number_loaded_rows, Some(22931));
    assert_eq!(response.number_filtered_rows, Some(0));

    // Verify row count in StarRocks
    let count = get_table_count("weatherdata");
    assert_eq!(count, 22931);
    println!(
        "Weatherdata CSV loaded successfully: {} rows verified.",
        count
    );
}

#[tokio::test]
async fn test_e2e_csv_crashdata() {
    if !is_e2e_enabled() {
        println!("Skipping E2E test: STARROCKS_E2E is not set.");
        return;
    }

    truncate_table("crashdata");

    ensure_decompressed("NYPD_Crash_Data.csv");
    let config = get_config();
    let props = StreamLoadTableProperties::builder()
        .table("crashdata")
        .format(DataFormat::CSV)
        .column_separator(",")
        .skip_header(1)
        .header("enclose", "\"")
        .max_filter_ratio(1.0)
        .columns("tmp_CRASH_DATE, tmp_CRASH_TIME, CRASH_DATE=str_to_date(concat_ws(' ', tmp_CRASH_DATE, tmp_CRASH_TIME), '%m/%d/%Y %H:%i'),BOROUGH,ZIP_CODE,LATITUDE,LONGITUDE,LOCATION,ON_STREET_NAME,CROSS_STREET_NAME,OFF_STREET_NAME,NUMBER_OF_PERSONS_INJURED,NUMBER_OF_PERSONS_KILLED,NUMBER_OF_PEDESTRIANS_INJURED,NUMBER_OF_PEDESTRIANS_KILLED,NUMBER_OF_CYCLIST_INJURED,NUMBER_OF_CYCLIST_KILLED,NUMBER_OF_MOTORIST_INJURED,NUMBER_OF_MOTORIST_KILLED,CONTRIBUTING_FACTOR_VEHICLE_1,CONTRIBUTING_FACTOR_VEHICLE_2,CONTRIBUTING_FACTOR_VEHICLE_3,CONTRIBUTING_FACTOR_VEHICLE_4,CONTRIBUTING_FACTOR_VEHICLE_5,COLLISION_ID,VEHICLE_TYPE_CODE_1,VEHICLE_TYPE_CODE_2,VEHICLE_TYPE_CODE_3,VEHICLE_TYPE_CODE_4,VEHICLE_TYPE_CODE_5")
        .build();

    let manager = StreamLoadManager::new(config, props).expect("Failed to create manager");

    let data_bytes = std::fs::read("test_data/NYPD_Crash_Data.csv")
        .map(Bytes::from)
        .expect("Failed to read test dataset file");

    println!("Loading crashdata CSV ({} bytes)...", data_bytes.len());
    let response = manager
        .send_single_batch("crash-csv-e2e", data_bytes)
        .await
        .expect("Stream load failed");

    assert_eq!(response.status, "Success");
    assert_eq!(response.number_loaded_rows, Some(423725));
    assert_eq!(response.number_filtered_rows, Some(1));

    // Verify row count in StarRocks
    let count = get_table_count("crashdata");
    assert_eq!(count, 423725);
    println!(
        "Crashdata CSV loaded successfully: {} rows verified.",
        count
    );
}

#[tokio::test]
async fn test_e2e_json_weatherdata() {
    if !is_e2e_enabled() {
        println!("Skipping E2E test: STARROCKS_E2E is not set.");
        return;
    }

    truncate_table("weatherdata");

    ensure_decompressed("weatherdata.json");
    let config = get_config();
    let props = StreamLoadTableProperties::builder()
        .table("weatherdata")
        .format(DataFormat::JSON)
        .strip_outer_array(true)
        .build();

    let manager = StreamLoadManager::new(config, props).expect("Failed to create manager");

    let data_bytes = std::fs::read("test_data/weatherdata.json")
        .map(Bytes::from)
        .expect("Failed to read test dataset file");

    println!("Loading weatherdata JSON ({} bytes)...", data_bytes.len());
    let response = manager
        .send_single_batch("weather-json-e2e", data_bytes)
        .await
        .expect("Stream load failed");

    assert_eq!(response.status, "Success");
    assert_eq!(response.number_loaded_rows, Some(22931));
    assert_eq!(response.number_filtered_rows, Some(0));

    // Verify row count in StarRocks
    let count = get_table_count("weatherdata");
    assert_eq!(count, 22931);
    println!(
        "Weatherdata JSON loaded successfully: {} rows verified.",
        count
    );
}

#[tokio::test]
async fn test_e2e_json_crashdata() {
    if !is_e2e_enabled() {
        println!("Skipping E2E test: STARROCKS_E2E is not set.");
        return;
    }

    truncate_table("crashdata");

    ensure_decompressed("crashdata.json");
    let config = get_config();
    let props = StreamLoadTableProperties::builder()
        .table("crashdata")
        .format(DataFormat::JSON)
        .strip_outer_array(true)
        .ignore_json_size(true)
        .build();

    let manager = StreamLoadManager::new(config, props).expect("Failed to create manager");

    let data_bytes = std::fs::read("test_data/crashdata.json")
        .map(Bytes::from)
        .expect("Failed to read test dataset file");

    println!("Loading crashdata JSON ({} bytes)...", data_bytes.len());
    let response = manager
        .send_single_batch("crash-json-e2e", data_bytes)
        .await
        .expect("Stream load failed");

    assert_eq!(response.status, "Success");
    assert_eq!(response.number_loaded_rows, Some(423726));
    assert_eq!(response.number_filtered_rows, Some(0));

    // Verify row count in StarRocks
    let count = get_table_count("crashdata");
    assert_eq!(count, 423726);
    println!(
        "Crashdata JSON loaded successfully: {} rows verified.",
        count
    );
}
