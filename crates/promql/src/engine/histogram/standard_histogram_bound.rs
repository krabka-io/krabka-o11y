// Bucket constants and the last finite-bucket rule are from Prometheus
// v3.14.0 model/histogram/generic.go::getBoundExponential/exponentialBounds.
// Every lower-schema table is an exact stride of this 256-entry schema-8 table.
// Using powf instead changes observable iterator endpoints by one ULP.
const EXPONENTIAL_BOUNDS: [f64; 256] = [
    f64::from_bits(0x3fe0_0000_0000_0000),
    f64::from_bits(0x3fe0_0b1a_fa5a_bcbf),
    f64::from_bits(0x3fe0_163d_a9fb_3335),
    f64::from_bits(0x3fe0_2168_143b_0281),
    f64::from_bits(0x3fe0_2c9a_3e77_8061),
    f64::from_bits(0x3fe0_37d4_2e11_bbcc),
    f64::from_bits(0x3fe0_4315_e86e_7f84),
    f64::from_bits(0x3fe0_4e5f_72f6_54b1),
    f64::from_bits(0x3fe0_59b0_d315_8574),
    f64::from_bits(0x3fe0_650a_0e3c_1f89),
    f64::from_bits(0x3fe0_706b_29dd_f6dd),
    f64::from_bits(0x3fe0_7bd4_2b72_a836),
    f64::from_bits(0x3fe0_8745_1875_9bc8),
    f64::from_bits(0x3fe0_92bd_f666_07e0),
    f64::from_bits(0x3fe0_9e3e_cac6_f383),
    f64::from_bits(0x3fe0_a9c7_9b1f_3919),
    f64::from_bits(0x3fe0_b558_6cf9_890f),
    f64::from_bits(0x3fe0_c0f1_45e4_6c85),
    f64::from_bits(0x3fe0_cc92_2b72_47f6),
    f64::from_bits(0x3fe0_d83b_2339_5dea),
    f64::from_bits(0x3fe0_e3ec_32d3_d1a2),
    f64::from_bits(0x3fe0_efa5_5fdf_a9c5),
    f64::from_bits(0x3fe0_fb66_affe_d31a),
    f64::from_bits(0x3fe1_0730_28d7_233d),
    f64::from_bits(0x3fe1_1301_d012_5b50),
    f64::from_bits(0x3fe1_1edb_ab5e_2ab5),
    f64::from_bits(0x3fe1_2abd_c06c_31cb),
    f64::from_bits(0x3fe1_36a8_14f2_04aa),
    f64::from_bits(0x3fe1_429a_aea9_2ddf),
    f64::from_bits(0x3fe1_4e95_934f_312d),
    f64::from_bits(0x3fe1_5a98_c8a5_8e50),
    f64::from_bits(0x3fe1_66a4_5471_c3c1),
    f64::from_bits(0x3fe1_72b8_3c7d_517b),
    f64::from_bits(0x3fe1_7ed4_8695_bbc0),
    f64::from_bits(0x3fe1_8af9_388c_8de9),
    f64::from_bits(0x3fe1_9726_5837_5d2f),
    f64::from_bits(0x3fe1_a35b_eb6f_cb75),
    f64::from_bits(0x3fe1_af99_f813_8a1c),
    f64::from_bits(0x3fe1_bbe0_8404_5cd3),
    f64::from_bits(0x3fe1_c82f_9528_1c6b),
    f64::from_bits(0x3fe1_d487_3168_b9aa),
    f64::from_bits(0x3fe1_e0e7_5eb4_4026),
    f64::from_bits(0x3fe1_ed50_22fc_d91c),
    f64::from_bits(0x3fe1_f9c1_8438_ce4c),
    f64::from_bits(0x3fe2_063b_8862_8cd6),
    f64::from_bits(0x3fe2_12be_3578_a819),
    f64::from_bits(0x3fe2_1f49_917d_dc95),
    f64::from_bits(0x3fe2_2bdd_a279_12d0),
    f64::from_bits(0x3fe2_387a_6e75_6238),
    f64::from_bits(0x3fe2_451f_fb82_140a),
    f64::from_bits(0x3fe2_51ce_4fb2_a63e),
    f64::from_bits(0x3fe2_5e85_711e_ce74),
    f64::from_bits(0x3fe2_6b45_65e2_7cdd),
    f64::from_bits(0x3fe2_780e_341d_df2a),
    f64::from_bits(0x3fe2_84df_e1f5_6380),
    f64::from_bits(0x3fe2_91ba_7591_bb6f),
    f64::from_bits(0x3fe2_9e9d_f51f_dee1),
    f64::from_bits(0x3fe2_ab8a_66d1_0f13),
    f64::from_bits(0x3fe2_b87f_d0da_d98f),
    f64::from_bits(0x3fe2_c57e_3977_1b2e),
    f64::from_bits(0x3fe2_d285_a6e4_030b),
    f64::from_bits(0x3fe2_df96_1f64_1589),
    f64::from_bits(0x3fe2_ecaf_a93e_2f55),
    f64::from_bits(0x3fe2_f9d2_4abd_886a),
    f64::from_bits(0x3fe3_06fe_0a31_b715),
    f64::from_bits(0x3fe3_1432_edee_b2fd),
    f64::from_bits(0x3fe3_2170_fc4c_d831),
    f64::from_bits(0x3fe3_2eb8_3ba8_ea32),
    f64::from_bits(0x3fe3_3c08_b264_16ff),
    f64::from_bits(0x3fe3_4962_66e3_fa2d),
    f64::from_bits(0x3fe3_56c5_5f92_9ff0),
    f64::from_bits(0x3fe3_6431_a2de_883a),
    f64::from_bits(0x3fe3_71a7_373a_a9ca),
    f64::from_bits(0x3fe3_7f26_231e_7549),
    f64::from_bits(0x3fe3_8cae_6d05_d864),
    f64::from_bits(0x3fe3_9a40_1b71_40ed),
    f64::from_bits(0x3fe3_a7db_34e5_9ff6),
    f64::from_bits(0x3fe3_b57f_bfec_6cf4),
    f64::from_bits(0x3fe3_c32d_c313_a8e3),
    f64::from_bits(0x3fe3_d0e5_44ed_e172),
    f64::from_bits(0x3fe3_dea6_4c12_3422),
    f64::from_bits(0x3fe3_ec70_df1c_5175),
    f64::from_bits(0x3fe3_fa45_04ac_801b),
    f64::from_bits(0x3fe4_0822_c367_a024),
    f64::from_bits(0x3fe4_160a_21f7_2e2a),
    f64::from_bits(0x3fe4_23fb_2709_468a),
    f64::from_bits(0x3fe4_31f5_d950_a896),
    f64::from_bits(0x3fe4_3ffa_3f84_b9d4),
    f64::from_bits(0x3fe4_4e08_6061_892d),
    f64::from_bits(0x3fe4_5c20_42a7_d232),
    f64::from_bits(0x3fe4_6a41_ed1d_0057),
    f64::from_bits(0x3fe4_786d_668b_3236),
    f64::from_bits(0x3fe4_86a2_b5c1_3cd0),
    f64::from_bits(0x3fe4_94e1_e192_aed2),
    f64::from_bits(0x3fe4_a32a_f0d7_d3de),
    f64::from_bits(0x3fe4_b17d_ea6d_b7d7),
    f64::from_bits(0x3fe4_bfda_d536_2a27),
    f64::from_bits(0x3fe4_ce41_b817_c114),
    f64::from_bits(0x3fe4_dcb2_99fd_dd0d),
    f64::from_bits(0x3fe4_eb2d_81d8_abfe),
    f64::from_bits(0x3fe4_f9b2_769d_2ca7),
    f64::from_bits(0x3fe5_0841_7f45_31ef),
    f64::from_bits(0x3fe5_16da_a2cf_6642),
    f64::from_bits(0x3fe5_257d_e83f_4eef),
    f64::from_bits(0x3fe5_342b_569d_4f81),
    f64::from_bits(0x3fe5_42e2_f4f6_ad27),
    f64::from_bits(0x3fe5_51a4_ca5d_920d),
    f64::from_bits(0x3fe5_6070_dde9_10d0),
    f64::from_bits(0x3fe5_6f47_36b5_27da),
    f64::from_bits(0x3fe5_7e27_dbe2_c4cf),
    f64::from_bits(0x3fe5_8d12_d497_c7fc),
    f64::from_bits(0x3fe5_9c08_27ff_07cb),
    f64::from_bits(0x3fe5_ab07_dd48_5429),
    f64::from_bits(0x3fe5_ba11_fba8_7a02),
    f64::from_bits(0x3fe5_c926_8a59_46b6),
    f64::from_bits(0x3fe5_d845_9099_8b92),
    f64::from_bits(0x3fe5_e76f_15ad_2148),
    f64::from_bits(0x3fe5_f6a3_20dc_eb71),
    f64::from_bits(0x3fe6_05e1_b976_dc08),
    f64::from_bits(0x3fe6_152a_e6cd_f6f4),
    f64::from_bits(0x3fe6_247e_b03a_5584),
    f64::from_bits(0x3fe6_33dd_1d19_29fd),
    f64::from_bits(0x3fe6_4346_34cc_c31e),
    f64::from_bits(0x3fe6_52b9_febc_8fb5),
    f64::from_bits(0x3fe6_6238_8255_2224),
    f64::from_bits(0x3fe6_71c1_c708_33f5),
    f64::from_bits(0x3fe6_8155_d44c_a972),
    f64::from_bits(0x3fe6_90f4_b19e_9537),
    f64::from_bits(0x3fe6_a09e_667f_3bcc),
    f64::from_bits(0x3fe6_b052_fa75_173e),
    f64::from_bits(0x3fe6_c012_750b_dabe),
    f64::from_bits(0x3fe6_cfdc_ddd4_7645),
    f64::from_bits(0x3fe6_dfb2_3c65_1a2e),
    f64::from_bits(0x3fe6_ef92_9859_3ae4),
    f64::from_bits(0x3fe6_ff7d_f951_9482),
    f64::from_bits(0x3fe7_0f74_66f4_2e85),
    f64::from_bits(0x3fe7_1f75_e8ec_5f73),
    f64::from_bits(0x3fe7_2f82_86ea_d089),
    f64::from_bits(0x3fe7_3f9a_48a5_8172),
    f64::from_bits(0x3fe7_4fbd_35d7_cbfc),
    f64::from_bits(0x3fe7_5feb_5642_67c8),
    f64::from_bits(0x3fe7_7024_b1ab_6e09),
    f64::from_bits(0x3fe7_8069_4fde_5d3e),
    f64::from_bits(0x3fe7_90b9_38ac_1cf5),
    f64::from_bits(0x3fe7_a114_73eb_0186),
    f64::from_bits(0x3fe7_b17b_0976_cfda),
    f64::from_bits(0x3fe7_c1ed_0130_c131),
    f64::from_bits(0x3fe7_d26a_62ff_86ef),
    f64::from_bits(0x3fe7_e2f3_36cf_4e61),
    f64::from_bits(0x3fe7_f387_8491_c490),
    f64::from_bits(0x3fe8_0427_543e_1a10),
    f64::from_bits(0x3fe8_14d2_add1_06d8),
    f64::from_bits(0x3fe8_2589_994c_ce11),
    f64::from_bits(0x3fe8_364c_1eb9_41f6),
    f64::from_bits(0x3fe8_471a_4623_c7ab),
    f64::from_bits(0x3fe8_57f4_179f_5b1f),
    f64::from_bits(0x3fe8_68d9_9b44_92eb),
    f64::from_bits(0x3fe8_79ca_d931_a435),
    f64::from_bits(0x3fe8_8ac7_d98a_6697),
    f64::from_bits(0x3fe8_9bd0_a478_580d),
    f64::from_bits(0x3fe8_ace5_422a_a0db),
    f64::from_bits(0x3fe8_be05_bad6_1778),
    f64::from_bits(0x3fe8_cf32_16b5_448b),
    f64::from_bits(0x3fe8_e06a_5e08_66d8),
    f64::from_bits(0x3fe8_f1ae_9915_7736),
    f64::from_bits(0x3fe9_02fe_d028_2c8a),
    f64::from_bits(0x3fe9_145b_0b91_ffc5),
    f64::from_bits(0x3fe9_25c3_53aa_2fe2),
    f64::from_bits(0x3fe9_3737_b0cd_c5e4),
    f64::from_bits(0x3fe9_48b8_2b5f_98e4),
    f64::from_bits(0x3fe9_5a44_cbc8_520d),
    f64::from_bits(0x3fe9_6bdd_9a76_70b1),
    f64::from_bits(0x3fe9_7d82_9fde_4e4f),
    f64::from_bits(0x3fe9_8f33_e47a_22a2),
    f64::from_bits(0x3fe9_a0f1_70ca_07b8),
    f64::from_bits(0x3fe9_b2bb_4d53_fe0b),
    f64::from_bits(0x3fe9_c491_82a3_f08f),
    f64::from_bits(0x3fe9_d674_194b_b8d4),
    f64::from_bits(0x3fe9_e863_19e3_2321),
    f64::from_bits(0x3fe9_fa5e_8d07_f29c),
    f64::from_bits(0x3fea_0c66_7b5d_e564),
    f64::from_bits(0x3fea_1e7a_ed8e_b8bb),
    f64::from_bits(0x3fea_309b_ec4a_2d32),
    f64::from_bits(0x3fea_42c9_8046_0ad7),
    f64::from_bits(0x3fea_5503_b23e_255b),
    f64::from_bits(0x3fea_674a_8af4_6051),
    f64::from_bits(0x3fea_799e_1330_b356),
    f64::from_bits(0x3fea_8bfe_53c1_2e57),
    f64::from_bits(0x3fea_9e6b_5579_fdbe),
    f64::from_bits(0x3fea_b0e5_2135_6eb9),
    f64::from_bits(0x3fea_c36b_bfd3_f378),
    f64::from_bits(0x3fea_d5ff_3a3c_2773),
    f64::from_bits(0x3fea_e89f_995a_d3ac),
    f64::from_bits(0x3fea_fb4c_e622_f2fe),
    f64::from_bits(0x3feb_0e07_298d_b663),
    f64::from_bits(0x3feb_20ce_6c9a_8950),
    f64::from_bits(0x3feb_33a2_b84f_15fa),
    f64::from_bits(0x3feb_4684_15b7_49b0),
    f64::from_bits(0x3feb_5972_8de5_5938),
    f64::from_bits(0x3feb_6c6e_29f1_c529),
    f64::from_bits(0x3feb_7f76_f2fb_5e45),
    f64::from_bits(0x3feb_928c_f227_49e2),
    f64::from_bits(0x3feb_a5b0_30a1_0647),
    f64::from_bits(0x3feb_b8e0_b79a_6f1d),
    f64::from_bits(0x3feb_cc1e_904b_c1d0),
    f64::from_bits(0x3feb_df69_c3f3_a205),
    f64::from_bits(0x3feb_f2c2_5bd7_1e06),
    f64::from_bits(0x3fec_0628_6141_b33b),
    f64::from_bits(0x3fec_199b_dd85_529a),
    f64::from_bits(0x3fec_2d1c_d9fa_652a),
    f64::from_bits(0x3fec_40ab_5fff_d078),
    f64::from_bits(0x3fec_5447_78fa_fb20),
    f64::from_bits(0x3fec_67f1_2e57_d149),
    f64::from_bits(0x3fec_7ba8_8988_c931),
    f64::from_bits(0x3fec_8f6d_9406_e7b2),
    f64::from_bits(0x3fec_a340_5751_c4d8),
    f64::from_bits(0x3fec_b720_dcef_9066),
    f64::from_bits(0x3fec_cb0f_2e6d_1672),
    f64::from_bits(0x3fec_df0b_555d_c3f6),
    f64::from_bits(0x3fec_f315_5b5b_ab70),
    f64::from_bits(0x3fed_072d_4a07_8979),
    f64::from_bits(0x3fed_1b53_2b08_c966),
    f64::from_bits(0x3fed_2f87_080d_89ee),
    f64::from_bits(0x3fed_43c8_eaca_a1d3),
    f64::from_bits(0x3fed_5818_dcfb_a486),
    f64::from_bits(0x3fed_6c76_e862_e6d2),
    f64::from_bits(0x3fed_80e3_16c9_8396),
    f64::from_bits(0x3fed_955d_71ff_6074),
    f64::from_bits(0x3fed_a9e6_03db_3284),
    f64::from_bits(0x3fed_be7c_d63a_8313),
    f64::from_bits(0x3fed_d321_f301_b45e),
    f64::from_bits(0x3fed_e7d5_641c_0656),
    f64::from_bits(0x3fed_fc97_337b_9b5d),
    f64::from_bits(0x3fee_1167_6b19_7d16),
    f64::from_bits(0x3fee_2646_14f5_a126),
    f64::from_bits(0x3fee_3b33_3b16_ee0f),
    f64::from_bits(0x3fee_502e_e78b_3ff5),
    f64::from_bits(0x3fee_6539_2467_6d75),
    f64::from_bits(0x3fee_7a51_fbc7_4c81),
    f64::from_bits(0x3fee_8f79_77cd_b73e),
    f64::from_bits(0x3fee_a4af_a2a4_90d8),
    f64::from_bits(0x3fee_b9f4_867c_ca6d),
    f64::from_bits(0x3fee_cf48_2d8e_67ee),
    f64::from_bits(0x3fee_e4aa_a218_850e),
    f64::from_bits(0x3fee_fa1b_ee61_5a26),
    f64::from_bits(0x3fef_0f9c_1cb6_4129),
    f64::from_bits(0x3fef_252b_376b_ba95),
    f64::from_bits(0x3fef_3ac9_48dd_7272),
    f64::from_bits(0x3fef_5076_5b6e_453e),
    f64::from_bits(0x3fef_6632_7988_44f6),
    f64::from_bits(0x3fef_7bfd_ad9c_be10),
    f64::from_bits(0x3fef_91d8_0224_3c86),
    f64::from_bits(0x3fef_a7c1_819e_90d6),
    f64::from_bits(0x3fef_bdba_3692_d512),
    f64::from_bits(0x3fef_d3c2_2b8f_71ee),
    f64::from_bits(0x3fef_e9d9_6b2a_23d6),
];

pub(crate) fn standard_histogram_bound(index: i32, schema: i8) -> f64 {
    bound(index, i32::from(schema))
}

pub(super) fn bound(index: i32, schema: i32) -> f64 {
    if schema < 0 {
        let shift = u32::try_from(-i64::from(schema)).unwrap_or(u32::MAX);
        let exponent = i64::from(index).checked_shl(shift).unwrap_or(0);
        return if exponent == 1024 {
            f64::MAX
        } else {
            scale(1.0, exponent)
        };
    }
    // Query-produced histograms have schemas <=8; resolution only decreases.
    let shift = u32::try_from(schema).expect("positive schema");
    let mask = (1_i32 << shift) - 1;
    let fraction_index =
        usize::try_from(index & mask).expect("positive fraction index") << (8 - shift);
    let fraction = EXPONENTIAL_BOUNDS[fraction_index];
    let exponent = (i64::from(index) >> shift) + 1;
    if fraction.partial_cmp(&0.5) == Some(std::cmp::Ordering::Equal) && exponent == 1025 {
        return f64::MAX;
    }
    scale(fraction, exponent)
}
fn scale(fraction: f64, exponent: i64) -> f64 {
    if exponent > 1024 {
        return f64::INFINITY;
    }
    if exponent < -1074 {
        return 0.0;
    }
    let power = |exponent: i64| {
        f64::from_bits(u64::try_from(exponent + 1023).expect("normal exponent") << 52)
    };
    if exponent == 1024 {
        (fraction * power(1023)) * 2.0
    } else if exponent < -1022 {
        (fraction * power(exponent + 1074)) * f64::from_bits(1)
    } else {
        fraction * power(exponent)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pinned_bounds_keep_rounding_subnormals_and_last_finite_bucket() {
        // Go 1.26.5 source results. Negative indices exercise table wrapping.
        for (index, schema, expected) in [
            (-1, 1, f64::from_bits(0x3fe6_a09e_667f_3bcc)),
            (-1, 8, f64::from_bits(0x3fef_e9d9_6b2a_23d6)),
            (1, 8, 1.002_711_275_050_202_5),
            (1024, 0, f64::MAX),
            (262_144, 8, f64::MAX),
            (1025, 0, f64::INFINITY),
            (-1074, 0, f64::from_bits(1)),
            (-1075, 0, 0.0),
        ] {
            assert2::assert!(
                standard_histogram_bound(index, schema).to_bits() == expected.to_bits()
            );
        }
        assert2::assert!(bound(1, -1000).to_bits() == 1.0_f64.to_bits());
    }
}
