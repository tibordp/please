use please::commands::{jgrep, window};
use please::*;
use regex::Regex;
use std::io::Write;
use tempfile::NamedTempFile;

#[tokio::test]
async fn test_file_or_std_basic() {
    use std::str::FromStr;

    let std_input = FileOrStd::from_str("-").unwrap();
    assert!(matches!(std_input, FileOrStd::Std));

    let file_input = FileOrStd::from_str("/path/to/file").unwrap();
    assert!(matches!(file_input, FileOrStd::File(_)));
}

#[tokio::test]
async fn test_field_entry_equality() {
    let entry1 = FieldEntry {
        line: "test line".to_string(),
        field: Some("field".to_string()),
    };
    let entry2 = FieldEntry {
        line: "different line".to_string(),
        field: Some("field".to_string()),
    };

    assert_eq!(entry1, entry2); // Should be equal based on field
}

#[test]
fn test_cache_key_generation() {
    let command1 = vec!["echo".to_string(), "hello".to_string()];
    let command2 = vec!["echo".to_string(), "hello".to_string()];
    let command3 = vec!["echo".to_string(), "world".to_string()];

    let key1 = get_cache_key(&command1).unwrap();
    let key2 = get_cache_key(&command2).unwrap();
    let key3 = get_cache_key(&command3).unwrap();

    assert_eq!(key1, key2); // Same commands should produce same keys
    assert_ne!(key1, key3); // Different commands should produce different keys
}

async fn create_test_file(content: &str) -> NamedTempFile {
    let mut temp_file = NamedTempFile::new().unwrap();
    temp_file.write_all(content.as_bytes()).unwrap();
    temp_file.flush().unwrap();
    temp_file
}

#[tokio::test]
async fn test_basic_commands() {
    let temp_file = create_test_file("line1\nline2\nline3\n").await;
    let file_input = FileOrStd::File(temp_file.path().to_path_buf());

    // Test skip command
    let result = skip(file_input, 1).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_tally_functionality() {
    let temp_file = create_test_file("apple\nbanana\napple\ncherry\n").await;
    let file_input = FileOrStd::File(temp_file.path().to_path_buf());

    let field_delimiter = Regex::new(r",").unwrap();
    let result = tally_impl(
        file_input,
        Sort::Desc,
        ",".to_string(),
        field_delimiter,
        0,
        None,
    )
    .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_replace_functionality() {
    let temp_file = create_test_file("hello world\nfoo bar\n").await;
    let file_input = FileOrStd::File(temp_file.path().to_path_buf());

    let regex = Regex::new(r"hello").unwrap();
    let result = replace(file_input, regex, "hi").await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_merge_functionality() {
    let temp_file1 = create_test_file("line1\nline2\n").await;
    let temp_file2 = create_test_file("line3\nline4\n").await;

    let files = vec![
        FileOrStd::File(temp_file1.path().to_path_buf()),
        FileOrStd::File(temp_file2.path().to_path_buf()),
    ];

    let result = merge(files).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_where_command_new_api() {
    let temp_file = create_test_file("apple,5\nbanana,3\ncherry,8\ndate,2\n").await;
    let file_input = FileOrStd::File(temp_file.path().to_path_buf());

    let delimiter = Regex::new(r",").unwrap();
    let conditions = WhereConditions {
        eq: None,
        ne: None,
        lt: None,
        le: None,
        gt: Some("4".to_string()),
        ge: None,
        contains: None,
        matches: None,
    };
    let result = where_filter(file_input, 2, conditions, delimiter).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_sort_command() {
    let temp_file = create_test_file("banana,3\napple,5\ndate,2\ncherry,8\n").await;
    let file_input = FileOrStd::File(temp_file.path().to_path_buf());

    let delimiter = Regex::new(r",").unwrap();
    let result = sort_lines(file_input, 2, SortType::Numeric, false, delimiter).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_group_command_new_api() {
    let temp_file = create_test_file("apple,red,5\nbanana,yellow,3\ncherry,red,8\n").await;
    let file_input = FileOrStd::File(temp_file.path().to_path_buf());

    let delimiter = Regex::new(r",").unwrap();
    let aggregations = GroupAggregations {
        count: false,
        distinct_count: None,
        sum: Some(3),
        avg: None,
        min: None,
        max: None,
        first: None,
        last: None,
        values: None,
        distinct_values: None,
    };
    let result = group_by(file_input, 2, aggregations, delimiter, ",".to_string()).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_group_dedup() {
    let temp_file = create_test_file("apple\nbanana\napple\ncherry\nbanana\n").await;
    let file_input = FileOrStd::File(temp_file.path().to_path_buf());

    let delimiter = Regex::new(r",").unwrap();
    let aggregations = GroupAggregations {
        count: false,
        distinct_count: None,
        sum: None,
        avg: None,
        min: None,
        max: None,
        first: None,
        last: None,
        values: None,
        distinct_values: None,
    };
    let result = group_by(file_input, 0, aggregations, delimiter, ",".to_string()).await;
    assert!(result.is_ok());
}

#[test]
fn test_where_condition_evaluation() {
    let condition = WhereCondition::new(2, CompareOp::Gt, "5".to_string()).unwrap();

    // Test numeric comparison (field 2 = index 1)
    assert!(condition.evaluate("test,10", &["test", "10"]));
    assert!(!condition.evaluate("test,3", &["test", "3"]));

    // Test string comparison fallback
    let condition = WhereCondition::new(2, CompareOp::Contains, "app".to_string()).unwrap();
    assert!(condition.evaluate("test,apple", &["test", "apple"]));
    assert!(!condition.evaluate("test,banana", &["test", "banana"]));
}

#[tokio::test]
async fn test_group_distinct_count() {
    let temp_file =
        create_test_file("apple,red,5\nbanana,red,3\ncherry,red,8\ngrape,blue,2\n").await;
    let file_input = FileOrStd::File(temp_file.path().to_path_buf());

    let delimiter = Regex::new(r",").unwrap();
    let aggregations = GroupAggregations {
        count: false,
        distinct_count: Some(1), // Count distinct fruits per color
        sum: None,
        avg: None,
        min: None,
        max: None,
        first: None,
        last: None,
        values: None,
        distinct_values: None,
    };
    let result = group_by(file_input, 2, aggregations, delimiter, ",".to_string()).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_group_values() {
    let temp_file = create_test_file("apple,red,5\nbanana,yellow,3\ncherry,red,8\n").await;
    let file_input = FileOrStd::File(temp_file.path().to_path_buf());

    let delimiter = Regex::new(r",").unwrap();
    let aggregations = GroupAggregations {
        count: false,
        distinct_count: None,
        sum: None,
        avg: None,
        min: None,
        max: None,
        first: None,
        last: None,
        values: Some(1), // Get all fruits for each color
        distinct_values: None,
    };
    let result = group_by(file_input, 2, aggregations, delimiter, ",".to_string()).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_group_distinct_values() {
    let temp_file =
        create_test_file("apple,red,5\nbanana,yellow,3\ncherry,red,8\napple,red,7\n").await;
    let file_input = FileOrStd::File(temp_file.path().to_path_buf());

    let delimiter = Regex::new(r",").unwrap();
    let aggregations = GroupAggregations {
        count: false,
        distinct_count: None,
        sum: None,
        avg: None,
        min: None,
        max: None,
        first: None,
        last: None,
        values: None,
        distinct_values: Some(1), // Get distinct fruits for each color
    };
    let result = group_by(file_input, 2, aggregations, delimiter, ",".to_string()).await;
    assert!(result.is_ok());
}
#[tokio::test]
async fn test_window_basic() {
    let temp_file = create_test_file("line1\nline2\nline3\n").await;
    let file_input = FileOrStd::File(temp_file.path().to_path_buf());

    // Test that window function doesn't crash (hard to test interactive features in unit tests)
    let result = window(file_input, 3, 100).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_jgrep_basic() {
    let json_content = r#"{"users": [{"name": "alice", "theme": "dark"}, {"name": "bob", "theme": "light"}], "config": {"theme": "auto"}}"#;
    let temp_file = create_test_file(json_content).await;
    let file_input = FileOrStd::File(temp_file.path().to_path_buf());

    let result = jgrep(
        file_input,
        "dark".to_string(),
        false,
        false,
        false,
        false,
        0,
    )
    .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_jgrep_array_primitives() {
    let json_content =
        r#"{"themes": ["light", "dark", "auto"], "users": [{"name": "alice", "theme": "dark"}]}"#;
    let temp_file = create_test_file(json_content).await;
    let file_input = FileOrStd::File(temp_file.path().to_path_buf());

    let result = jgrep(
        file_input,
        "dark".to_string(),
        false,
        false,
        false,
        false,
        0,
    )
    .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_jgrep_keys_only() {
    let json_content = r#"{"user_data": {"name": "alice"}, "user_count": 5}"#;
    let temp_file = create_test_file(json_content).await;
    let file_input = FileOrStd::File(temp_file.path().to_path_buf());

    let result = jgrep(file_input, "user".to_string(), true, false, false, false, 0).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_jgrep_values_only() {
    let json_content = r#"{"user_data": {"name": "alice"}, "count": 5}"#;
    let temp_file = create_test_file(json_content).await;
    let file_input = FileOrStd::File(temp_file.path().to_path_buf());

    let result = jgrep(
        file_input,
        "alice".to_string(),
        false,
        true,
        false,
        false,
        0,
    )
    .await;
    assert!(result.is_ok());
}
