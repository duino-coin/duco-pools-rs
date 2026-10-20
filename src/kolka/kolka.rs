use crate::sync::sync::Curve;

// based on the values provided by the master server, this function calculates the estimated daily reward that should be given to a device of this hashrate
pub fn calculate_daily_reward(hashrate: f64, curve: &Curve) -> f64 {

    let phi = curve.gpu_base
        * (1.0
            + ((curve.gpu_growth
                * (1.0
                    + (hashrate / (curve.gpu_growth_start * curve.pc_gpu_crossover_h))
                        .powf(curve.gpu_growth_softness))
                .log2())
                / curve.gpu_growth_softness));

    phi + ((((((curve.pc_base
        + ((curve.mcu_base - curve.pc_base)
            / (1.0 + (hashrate / curve.mcu_pc_step_h).powf(curve.mcu_pc_step_sharpness))))
        * (1.0 + (curve.slow_boost_h / hashrate)))
        * (1.0 + (curve.arduino_h / hashrate).powf(curve.cutoff_sharpness))
            .powf(curve.cutoff_slope / curve.cutoff_sharpness))
        * (1.0
            + (curve.arduino_bonus
                / (1.0
                    + ((hashrate / curve.arduino_h).log2() / curve.arduino_bonus_width)
                        .powi(2)))))
        - phi)
        / (1.0 + (hashrate / curve.pc_gpu_crossover_h).powf(curve.pc_gpu_crossover_sharpness)))
}

pub fn calculate_difficulty(hashrate: f64, curve: &Curve) -> u32 {
    let diff = hashrate * curve.expected_sharetime as f64 / 50.0;
    (diff.round() as u32).max(1)
}

pub fn calculate_reward(hashrate: f32, amount_of_workers: f32, difficulty: u32, curve: &Curve) -> f32 {
    let drop = if hashrate as f64 > curve.h_ref { curve.drop_high } else { curve.drop_low };

    // the nonce is within `difficulty * 100`, so the average lies at roughly `difficulty * 50`
    // therefore, we expect a miner to solve a job at a given difficulty on average within `difficulty * 50 / hashrate`
    let average_expected_sharetime = difficulty as f32 * 50.0 / hashrate;
    // the reward per share is defined by how much of a day the sharetime makes up
    let reward = (calculate_daily_reward(hashrate as f64, curve) as f32 * average_expected_sharetime / 86_400.0)
        * drop.powi((amount_of_workers - 1.0) as i32) as f32;
    // a NaN would stick to the balance for good, a negative one would take coins away
    if reward.is_finite() { reward.max(0.0) } else { 0.0 }
}