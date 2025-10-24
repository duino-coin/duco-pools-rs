use serde_json::Value;
use std::sync::Arc;


#[derive(Clone)]
pub struct Kolka {
    highest_pc_diff: u32,
    highest_esp_diff: u32,
    highest_avr_diff: u32,
    gpu_mining_percentage: f32,
    pc_mining_percentage: f32,
    esp_mining_percentage: f32,
    avr_mining_percentage: f32,
}

impl Kolka {
    pub fn new(pool_rewards: &Arc<Value>) -> Self {
        Kolka {
            highest_pc_diff: (get_diff(pool_rewards, "NET") as f32 * 5.19202).round() as u32,
            highest_esp_diff: get_diff(pool_rewards, "ESP32"),
            highest_avr_diff: get_diff(pool_rewards, "DUE"),
            gpu_mining_percentage: get_decrease_perc(pool_rewards, "EXTREME") * 0.01,
            pc_mining_percentage: get_decrease_perc(pool_rewards, "NET") * 0.01,
            esp_mining_percentage: get_decrease_perc(pool_rewards, "ESP32") * 0.01,
            avr_mining_percentage: get_decrease_perc(pool_rewards, "AVR") * 0.01,
        }
    }
}


pub fn v1(
    kolka: &Arc<Kolka>,
    hashrate: f32,
    difficulty: u32,
    amount_of_workers: f32,
    reward_div: u32,
) -> f32 {
    let mut output = hashrate.log10() / reward_div as f32;
    if difficulty > kolka.highest_pc_diff {
        // GPU, Extreme PC
        output = 2f32
            * (output
            * kolka
            .gpu_mining_percentage
            .powi((amount_of_workers - 1.0) as i32));
    } else if difficulty > kolka.highest_esp_diff {
        // PC
        output = 2f32
            * (output
            * kolka
            .pc_mining_percentage
            .powi((amount_of_workers - 1.0) as i32));
    } else if difficulty > kolka.highest_avr_diff {
        // ESP
        output = 2f32
            * (output
            * kolka
            .esp_mining_percentage
            .powi((amount_of_workers - 1.0) as i32));
    } else {
        // AVR
        output = 2f32
            * (output
            * kolka
            .avr_mining_percentage
            .powi((amount_of_workers - 1.0) as i32));
    }
    output
}

pub fn v2(difficulty: &str) -> String {
    match difficulty {
        "AVR" => "MEGA",
        "MEGA" => "ARM",
        "ARM" => "DUE",
        "DUE" => "ESP8266",
        "ESP8266" => "ESP8266H",
        "ESP8266N" => "ESP8266NH",
        "ESP32" => "ESP32S",
        "LOW" => "MEDIUM",
        "MEDIUM" => "NET",
        "NET" => "EXTREME",
        _ => difficulty,
    }
        .to_string()
}

pub fn v2_reverse(difficulty: &str) -> String {
    match difficulty {
        "AVR" => "AVR",
        "MEGA" => "AVR",
        "ARM" => "MEGA",
        "DUE" => "ARM",
        "ESP8266NH" => "ESP8266N",
        "ESP8266H" => "ESP8266",
        "ESP32" => "ESP32",
        "MEDIUM" => "LOW",
        "NET" => "MEDIUM",
        "EXTREME" => "NET",
        _ => difficulty,
    }
        .to_string()
}
pub fn v3(sharetime: f32, expected_sharetime: u16, hashrate: f32) -> u32 {
    let mut new_diff =
        ((hashrate / 100f32).floor() * expected_sharetime as f32).round() as u32;

    let deviation = sharetime / expected_sharetime as f32;
    const HASHRATE_SCALING_FACTOR: f32 = 0.28;
    if deviation < 0.9 {
        new_diff *= (1.0 + HASHRATE_SCALING_FACTOR).round() as u32;
    } else if deviation > 1.0 {
        new_diff *= (1.0 - HASHRATE_SCALING_FACTOR).round() as u32;
    }
    const MIN_DIFF: u32 = 5000;
    new_diff = MIN_DIFF.max(new_diff);

    new_diff
}
pub fn v4(kolka: &Arc<Kolka>, sharetime: f32, expected_test_sharetime: u16) -> (bool, f32) {
    let p = sharetime / expected_test_sharetime as f32;
    if p > 1.5 {
        (true, v1(kolka,0f32, sharetime as u32, 0f32, 0)) // this doesn't make any sense
    } else {
        (false, 0f32)
    }
}
pub fn get_diff(pool_rewards: &Arc<Value>, req_diff: &str) -> u32 {
    let diff = pool_rewards
        .get(req_diff)
        .unwrap()
        .get("difficulty")
        .unwrap()
        .as_u64()
        .unwrap();
    diff as u32
}

fn get_decrease_perc(pool_rewards: &Arc<Value>, req_diff: &str) -> f32 {
    let diff = pool_rewards
        .get(req_diff)
        .unwrap()
        .get("kolka_decrease_perc")
        .unwrap()
        .as_f64()
        .unwrap();
    diff as f32
}
