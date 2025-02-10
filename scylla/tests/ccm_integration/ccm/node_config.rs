use core::str;
use serde_yaml::Value;
use std::collections::HashMap;

/// Represents data stored in scylla.yaml or cassandra.yaml
#[derive(Debug, Clone)]
pub(crate) enum NodeConfig {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    List(Vec<NodeConfig>),
    Map(HashMap<String, NodeConfig>),
}

impl Default for NodeConfig {
    fn default() -> NodeConfig {
        let mut val = HashMap::new();
        val.insert(
            "endpoint_snitch".to_string(),
            NodeConfig::String(
                "org.apache.cassandra.locator.GossipingPropertyFileSnitch".to_owned(),
            ),
        );
        NodeConfig::Map(val)
    }
}

#[allow(dead_code)]
impl NodeConfig {
    // Gets data by `path`
    // `path` is a dot-separated list of keys, example: obj1.key1
    // if it can't reach data by any reason it returns `default`
    pub(crate) fn get(&self, path: &str, default: NodeConfig) -> NodeConfig {
        let keys: Vec<&str> = path.split('.').collect();
        let mut current = self;

        for key in keys {
            match current {
                NodeConfig::Map(map) => {
                    if let Some(value) = map.get(key) {
                        current = value;
                    } else {
                        return default;
                    }
                }
                NodeConfig::List(list) => {
                    if let Ok(index) = key.parse::<usize>() {
                        if let Some(value) = list.get(index) {
                            current = value;
                        } else {
                            return default;
                        }
                    } else {
                        return default;
                    }
                }
                _ => return default,
            }
        }

        current.clone()
    }

    // Sets data by `path`
    // `path` is a dot-separated list of keys, example: obj1.key1
    // if it can't set data by any reason it returns false
    pub(crate) fn set(&mut self, path: &str, value: NodeConfig) -> bool {
        let keys: Vec<&str> = path.split('.').collect();
        let mut current = self;

        for (i, key) in keys.iter().enumerate() {
            match current {
                NodeConfig::Map(map) => {
                    if i == keys.len() - 1 {
                        map.insert(key.to_string(), value);
                        return true;
                    } else {
                        current = map
                            .entry(key.to_string())
                            .or_insert(NodeConfig::Map(HashMap::new()));
                    }
                }
                NodeConfig::List(list) => {
                    if let Ok(index) = key.parse::<usize>() {
                        if i == keys.len() - 1 {
                            if index < list.len() {
                                list[index] = value;
                                return true;
                            } else {
                                return false;
                            }
                        } else if index < list.len() {
                            current = &mut list[index];
                        } else {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                _ => return false,
            }
        }
        false
    }

    // Converts it to serde YAML representation
    pub(crate) fn to_yaml(&self) -> Value {
        match self {
            NodeConfig::Null => Value::Null,
            NodeConfig::Bool(b) => Value::Bool(*b),
            NodeConfig::Int(i) => Value::Number(serde_yaml::Number::from(*i)),
            NodeConfig::Float(f) => Value::Number(serde_yaml::Number::from(*f)),
            NodeConfig::String(s) => Value::String(s.clone()),
            NodeConfig::List(list) => {
                let yaml_list: Vec<Value> = list.iter().map(|item| item.to_yaml()).collect();
                Value::Sequence(yaml_list)
            }
            NodeConfig::Map(map) => {
                let yaml_map: serde_yaml::Mapping = map
                    .iter()
                    .map(|(key, value)| (Value::String(key.clone()), value.to_yaml()))
                    .collect();
                Value::Mapping(yaml_map)
            }
        }
    }

    /// Parses a serde YAML string into a NodeConfig structure
    pub(crate) fn from_yaml(value: Value) -> Result<NodeConfig, String> {
        match value {
            Value::Null => Ok(NodeConfig::Null),
            Value::Bool(b) => Ok(NodeConfig::Bool(b)),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(NodeConfig::Int(i))
                } else if let Some(f) = n.as_f64() {
                    Ok(NodeConfig::Float(f))
                } else {
                    Err("Number is not an integer or float".to_string())
                }
            }
            Value::String(s) => Ok(NodeConfig::String(s)),
            Value::Sequence(seq) => {
                let mut new_seq = Vec::new();
                for value in seq {
                    if let Ok(parsed_value) = NodeConfig::from_yaml(value) {
                        new_seq.push(parsed_value);
                    } else {
                        return Err("Error parsing value in sequence".to_string());
                    }
                }
                Ok(NodeConfig::List(new_seq))
            }
            Value::Mapping(map) => {
                let mut new_map = HashMap::new();
                for (key, value) in map {
                    if let Value::String(key_str) = key {
                        if let Ok(parsed_value) = NodeConfig::from_yaml(value) {
                            new_map.insert(key_str, parsed_value);
                        } else {
                            return Err("Error parsing value in mapping".to_string());
                        }
                    } else {
                        return Err("Invalid key type in mapping".to_string());
                    }
                }
                Ok(NodeConfig::Map(new_map))
            }
            _ => Err("Unsupported YAML type".to_string()), // Explicitly handle unsupported types
        }
    }

    // Represents config in format 'l1key1.l2key1:val1 l1key1.l2key2:val2 l1key3:val3'
    pub(crate) fn to_flat_string(&self) -> String {
        fn flatten_map(map: &HashMap<String, NodeConfig>, prefix: &str, output: &mut Vec<String>) {
            // Sort keys before processing
            let mut sorted_keys: Vec<&String> = map.keys().collect();
            sorted_keys.sort();

            for key in sorted_keys {
                let value = map.get(key).unwrap();
                let full_key = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{}.{}", prefix, key)
                };

                match value {
                    NodeConfig::Map(inner_map) => {
                        flatten_map(inner_map, full_key.as_str(), output);
                    }
                    NodeConfig::String(s) => {
                        output.push(format!("{}:{}", full_key, s));
                    }
                    NodeConfig::Int(i) => {
                        output.push(format!("{}:{}", full_key, i));
                    }
                    NodeConfig::Float(f) => {
                        output.push(format!("{}:{}", full_key, f));
                    }
                    NodeConfig::Bool(b) => {
                        output.push(format!("{}:{}", full_key, b));
                    }
                    NodeConfig::Null => {
                        output.push(format!("{}:null", full_key));
                    }
                    NodeConfig::List(list) => {
                        let list_str = list
                            .iter()
                            .map(|item| format!("{:?}", item))
                            .collect::<Vec<_>>()
                            .join(", ");
                        output.push(format!("{}:[{}]", full_key, list_str));
                    }
                }
            }
        }

        let mut result = Vec::new();
        if let NodeConfig::Map(map) = self {
            flatten_map(map, "", &mut result);
        }
        result.join(" ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_yaml::Value;

    #[test]
    fn test_from_yaml_and_to_yaml() {
        // Define a sample YAML string
        let yaml_str = r#"
            null_value: null
            bool_value: true
            int_value: 42
            float_value: 3.14
            string_value: "hello"
            list_value:
              - 1
              - 2
              - 3
            map_value:
              key1: "value1"
              key2: 99
        "#;

        // Parse the YAML string into a serde_yaml::Value
        let yaml_value: Value = serde_yaml::from_str(yaml_str).expect("Failed to parse YAML");

        // Convert from YAML to ClusterConfig
        let cluster_config = NodeConfig::from_yaml(yaml_value.clone())
            .expect("Failed to convert from YAML to ClusterConfig");

        // Convert back from ClusterConfig to YAML
        let converted_yaml_value = cluster_config.to_yaml();

        // Assert the conversion is accurate by comparing original and converted YAML values
        assert_eq!(yaml_value, converted_yaml_value);
    }

    #[test]
    fn test_to_yaml_empty_structures() {
        // Test empty list
        let empty_list = NodeConfig::List(vec![]);
        assert_eq!(empty_list.to_yaml(), Value::Sequence(vec![]));

        // Test empty map
        let empty_map = NodeConfig::Map(HashMap::new());
        assert_eq!(
            empty_map.to_yaml(),
            Value::Mapping(serde_yaml::Mapping::new())
        );
    }

    #[test]
    fn test_from_yaml_invalid_cases() {
        // Test unsupported YAML type (e.g., unhashable keys)
        let invalid_yaml = Value::Mapping({
            let mut map = serde_yaml::Mapping::new();
            map.insert(Value::Sequence(vec![]), Value::Null);
            map
        });

        let result = NodeConfig::from_yaml(invalid_yaml);
        assert!(result.is_err(), "Expected error for invalid YAML type");
    }

    #[test]
    fn test_to_flat_string_simple_map() {
        let mut map = HashMap::new();
        map.insert("key1".to_string(), NodeConfig::String("value1".to_string()));
        map.insert("key2".to_string(), NodeConfig::Int(42));

        let cluster_config = NodeConfig::Map(map);
        let flat_representation = cluster_config.to_flat_string();

        assert_eq!(flat_representation, "key1:value1 key2:42");
    }

    #[test]
    #[ignore]
    fn test_to_flat_string_nested_map() {
        let mut inner_map = HashMap::new();
        inner_map.insert("inner_key".to_string(), NodeConfig::Bool(true));

        let mut outer_map = HashMap::new();
        outer_map.insert("outer_key1".to_string(), NodeConfig::Map(inner_map));
        outer_map.insert("outer_key2".to_string(), NodeConfig::Float(3.1));

        let cluster_config = NodeConfig::Map(outer_map);
        let flat_representation = cluster_config.to_flat_string();

        assert_eq!(
            flat_representation,
            "outer_key1.inner_key:true outer_key2:3.14"
        );
    }

    #[test]
    fn test_to_flat_string_with_empty_map() {
        let empty_map = HashMap::new();
        let cluster_config = NodeConfig::Map(empty_map);
        let flat_representation = cluster_config.to_flat_string();

        assert_eq!(flat_representation, "");
    }

    #[test]
    fn test_to_flat_string_with_list() {
        let list = vec![
            NodeConfig::Int(1),
            NodeConfig::Int(2),
            NodeConfig::String("three".to_string()),
        ];

        let mut map = HashMap::new();
        map.insert("key_with_list".to_string(), NodeConfig::List(list));

        let cluster_config = NodeConfig::Map(map);
        let flat_representation = cluster_config.to_flat_string();

        // Lists are serialized as comma-separated values in brackets.
        assert_eq!(
            flat_representation,
            "key_with_list:[Int(1), Int(2), String(\"three\")]"
        );
    }

    #[test]
    fn test_to_flat_string_with_null() {
        let mut map = HashMap::new();
        map.insert("null_key".to_string(), NodeConfig::Null);

        let cluster_config = NodeConfig::Map(map);
        let flat_representation = cluster_config.to_flat_string();

        assert_eq!(flat_representation, "null_key:null");
    }
}
