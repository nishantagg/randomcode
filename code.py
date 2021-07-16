

"""
unit test case module for sgb_compass_payee_transformer class
"""

from __future__ import print_function
from sgb_compass_main.sgb_payee_main import SGBPayeeMain
from test_sgb_compass_payee_utils import *
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, BooleanType, TimestampType, DateType, BinaryType, ArrayType, \
    MapType
from datetime import datetime
import logging
import pyspark
import decimal
import json
from pyspark.sql import Window

pytestmark = pytest.mark.usefixtures("spark_session")


@pytest.fixture(scope='function')
def _framework_context():
    attrs = {"environment": "local",
             "mode": "client",
             "task_name": "transform",
             "app_id": "a00304",
             "odate": "20200831",
             "current_timestamp": "20200703",
             "transform_condition": "bdp_hour=00",
             "outbound_condition": "bdp_hour=00"
             }

    return create_kwargs_object(attrs)


def create_kwargs_object(attrs):
    """
    Returns an object with the specified dictionary as named attributes.
    :param attrs: Dictionary of attributes to attach to object.
    :return: Object with named attributes attached.
    """

    ctx = type('object', (), {})()
    for attr in attrs:
        setattr(ctx, attr, attrs[attr])
    return ctx


def test_when_sgb_compass_payee_main_transformer_process(_framework_context, spark_session_):
    tc = """<it should test sgb compass payee [MAIN] transformation process method should return target dataframe>"""
    '#Given'
    input_df_names = ["a00304_payees", "a00304_ttbeneficiaries", "a00304_billers", "a00304_npp_payid"]
    s_payee_df = sample_payee_df(spark_session_)
    s_tt_df = sample_tt_df(spark_session_)
    s_biller_df = sample_biller_df(spark_session_)
    s_biller_df.write.format("orc").mode('overwrite').save("C:\\Users\\L151557\\Office\\csv\\files\\biller")
    s_npp_df = sample_npp_df(spark_session_)
    s_npp_df.write.format("orc").mode('overwrite').save("C:\\Users\\L151557\\Office\\csv\\files\\npp")
    df_dict = {
        input_df_names[0]: s_payee_df,
        input_df_names[1]: s_tt_df,
        input_df_names[2]: s_biller_df,
        input_df_names[3]: s_npp_df,
    }

    obj_sgb_payee_main = SGBPayeeMain(_framework_context, spark_session_, "", df_dict)
    payee_target_df, payee_target_df1 = obj_sgb_payee_main.process()
    payee_target_df.show()
    payee_target_df1.show()
    '#Assertion for number of rows '
    payee_target_df.count() == (s_payee_df.count() + s_tt_df.count() + s_biller_df.count() + s_npp_df.count())


def test_when_changing_rates_transformer_process(_framework_context, spark_session_):
    df = sample_tt_df(spark_session_) \
        .withColumn("json_col", F.to_json(F.struct(F.array(F.struct(
        F.lit("FIXED").alias("depositRateType"),
        F.lit(4.12).cast(StringType()).alias("rate"),
        F.lit('MON').alias("calculationFrequency"),
        F.lit('TUY').alias("applicationFrequency"),
        F.array(F.struct(
            (F.lit("")).alias("name"),
            (F.lit("")).alias("unitOfMeasure"),
            (F.lit("")).alias("minimumValue"),
            (F.lit("")).alias("maximumValue"),
            (F.lit("")).alias("rateApplicationMethod"),
            F.struct(
                (F.lit("")).alias("additionalInfo"),
                (F.lit("")).alias("additionalInfoUri")
            ).alias("applicabilityConditions"),
            (F.lit("")).alias("additionalInfo"),
            (F.lit("")).alias("additionalInfoUri")
        )).alias("tiers")))))) \
        .withColumn("additionalValue", F.lit("")) \
        .withColumn("additionalInfo", F.lit("")) \
        .withColumn("additionalInfoUri", F.lit(""))

    df.show(5, False)


def test_1when_1changing_rates_transformer_process(_framework_context, spark_session_):
    def update_rates_json(lending_json_str):
        json_rates = json.loads(lending_json_str)
        for rates in json_rates["lendingRates"]:
            if (rates["rate"] != ""):
                rates["rate"] = str(float(rates["rate"]) / 100)
            else:
                rates["rate"] = "1.2"
        print(json_rates)
        return json.dumps(json_rates)

    update_rates_json_UDF = F.udf(update_rates_json, StringType())

    # json_sample = {"lendingRates": [{"t": 1434946093036, "rate": ""}, {"t": 1434946095013, "rate": "53.0"},
    #                          {"t": 1434946096823, "rate": "52.0"}]}
    #
    # for rates in json_sample["lendingRates"]:
    #     if (rates["rate"] != ""):
    #         rates["rate"] = str(float(rates["rate"]) / 100)
    #     else:
    #         rates["rate"] = "1.2"
    # print(json_sample)

    df = sample_payee_df(spark_session_) \
        .withColumn("lending_rates", F.unbase64(F.lit(
        'eyJsZW5kaW5nUmF0ZXMiOlt7ImxlbmRpbmdSYXRlVHlwZSI6IlBVUkNIQVNFIiwicmF0ZSI6IjAuMTgyNCIsImNvbXBhcmlzb25SYXRlIjoiIiwiY2FsY3VsYXRpb25GcmVxdWVuY3kiOiJQMUQiLCJhcHBsaWNhdGlvbkZyZXF1ZW5jeSI6IlAxTSIsImludGVyZXN0UGF5bWVudER1ZSI6IklOX0FSUkVBUlMiLCJyZXBheW1lbnRUeXBlIjoiUFJJTkNJUEFMX0FORF9JTlRFUkVTVCIsImxvYW5QdXJwb3NlIjoiIiwidGllcnMiOlt7Im5hbWUiOiIiLCJ1bml0T2ZNZWFzdXJlIjoiIiwibWluaW11bVZhbHVlIjoiIiwibWF4aW11bVZhbHVlIjoiIiwicmF0ZUFwcGxpY2F0aW9uTWV0aG9kIjoiIiwiYXBwbGljYWJpbGl0eUNvbmRpdGlvbnMiOnsiYWRkaXRpb25hbEluZm8iOiIiLCJhZGRpdGlvbmFsSW5mb1VyaSI6IiJ9LCJhZGRpdGlvbmFsSW5mbyI6IiIsImFkZGl0aW9uYWxJbmZvVXJpIjoiIn1dLCJhZGRpdGlvbmFsVmFsdWUiOiIiLCJhZGRpdGlvbmFsSW5mbyI6IkludGVyZXN0IG9uIFB1cmNoYXNlcyIsImFkZGl0aW9uYWxJbmZvVXJpIjoiIn1dfQ')).cast(
        StringType())) \
        .drop(SPayee.s_record_status) \
        .drop(SPayee.s_gcis_number) \
        .drop(SPayee.s_bsb_payee_account) \
        .drop(SPayee.s_payee_account_number) \
        .drop(SPayee.s_payee_name) \
        .drop(SPayee.s_payer_name) \
        .drop("creation_date_time")

    df_rates_modified = df.withColumn("lending_rates", update_rates_json_UDF(F.col("lending_rates")))
    print(" *********** Showing modified rates with Json. ******************")
    df_rates_modified.show(5, False)
    df_rates_modified_base64 = df_rates_modified.withColumn("lending_rates", F.base64(F.col("lending_rates")))
    print(" *********** Showing Converted back to BASE64. ******************")
    df_rates_modified_base64.show(5, False)
    df_uncodedbase_64 = df_rates_modified_base64.withColumn("lending_rates",
                                                            F.unbase64(F.col("lending_rates")).cast(StringType()))
    print(" *********** Showing uncoded from BASE64. to plain text. ******************")
    df_uncodedbase_64.show(5, False)
    df.show(5, False)


def test_with_real_scenario_transformer_process(_framework_context, spark_session_):
    def update_rates_json(lending_json_str):
        json_rates = json.loads(lending_json_str)
        for rates in json_rates["lendingRates"]:
            if (rates["rate"] != ""):
                rates["rate"] = str((decimal.Decimal(rates["rate"]) / 100))
            else:
                rates["rate"] = ""
        print(json_rates)
        return json.dumps(json_rates)

    update_rates_json_UDF = F.udf(update_rates_json, StringType())

    # spark_session_.sql("create database if not exists testdb")
    # spark_session_.sql("CREATE  EXTERNAL   TABLE  if not exists  testdb.a001fc_acct_detl_lending_deposit_rates_reference(lrrtrec  string) LOCATION  '/tmp/data/a001fc_acct_detl_lending_deposit_rates_reference'")
    # get_lending_rates = spark_session_.read.table("testdb.a001fc_acct_detl_lending_deposit_rates_reference")
    get_lending_rates = spark_session_.read.csv("C:\\tmp\\data_1\\a001fc_acct_detl_lending_deposit_rates_reference")
    # spark_session_.read.format("csv").load("C:\\tmp\\data\\a001fc_acct_detl_lending_deposit_rates_reference")
    get_lending_rates.show(5)
    get_lending_rates_uncoded = get_lending_rates.withColumn("lrrtrec", F.unbase64(F.col("_c0")).cast(StringType()))
    get_lending_rates_uncoded.show(200, False)

    df_rates_modified = get_lending_rates_uncoded.withColumn("lrrtrec", update_rates_json_UDF(F.col("lrrtrec")))
    print(" *********** Showing modified rates with Json. ******************")
    df_rates_modified.show(5, False)
    df_rates_modified.printSchema()

    df_rates_modified_base64 = df_rates_modified.withColumn("lrrtrec", F.base64(F.col("lrrtrec")))
    print(" *********** Showing Converted back to BASE64. ******************")
    df_rates_modified_base64.show(5, False)
    df_uncodedbase_64 = df_rates_modified_base64.withColumn("lrrtrec", F.unbase64(F.col("lrrtrec")).cast(StringType()))
    print(" *********** Showing uncoded from BASE64. to plain text. ******************")
    df_uncodedbase_64.show(5, False)

    jsonSchema = StructType([
        StructField("lendingRates", ArrayType(StructType([
            StructField("additionalInfo", StringType()),
            StructField("calculationFrequency", StringType()),
            StructField("applicationFrequency", StringType()),
            StructField("additionalValue", StringType()),
            StructField("repaymentType", StringType()),
            StructField("rate", StringType()),
            StructField("loanPurpose", StringType()),
            StructField("additionalInfoUri", StringType()),
            StructField("lendingRateType", StringType()),
            StructField("interestPaymentDue", StringType()),
            StructField("comparisonRate", StringType()),
            StructField("tiers", ArrayType(StructType([
                StructField("additionalInfo", StringType()),
                StructField("additionalInfo", StringType()),
                StructField("unitOfMeasure", StringType()),
                StructField("name", StringType()),
                StructField("maximumValue", StringType()),
                StructField("additionalInfoUri", StringType()),
                StructField("minimumValue", StringType()),
                StructField("rateApplicationMethod", StringType()),
                StructField("applicabilityConditions", StructType([
                    StructField("additionalInfoUri", StringType()),
                    StructField("additionalInfo", StringType())])
                            )
            ])))
        ])))
    ])

	
	
	
	
    df_uncodedbase_64 \
        .withColumn("structField", F.from_json(df_uncodedbase_64["lrrtrec"], jsonSchema)) \
        .printSchema()

    df_with_Json = df_uncodedbase_64 \
        .withColumn("structField", F.from_json(df_uncodedbase_64["lrrtrec"], jsonSchema))

    df_with_Json.select(df_with_Json["structField.lendingRates"].getItem("rate")).show()

    # df_rates_modified_base64.write.format("csv").mode('overwrite').save(
    #     "C:\\tmp\\data_1\\a001fc_acct_detl_lending_deposit_rates_reference")
    #


def when_scheck_map_values(spark_session_):
    #
    # dicts = spark_session_.sparkContext.broadcast(dict([
    #     ("AD", "AND"),
    #     ("AE", "ARE"),
    #     ("AF", "AFG"),
    #     ("AG", "ATG"),
    #     ("AI", "AIA"),
    #     ("AL", "ALB"),
    #     ("AM", "ARM"),
    #     ("AO", "AGO"),
    #     ("AQ", "ATA"),
    #     ("AR", "ARG"),
    #     ("AS", "ASM"),
    #     ("AT", "AUT"),
    #     ("AU", "AUS"),
    #     ("AW", "ABW"),
    #     ("AX", "ALA"),
    #     ("AZ", "AZE"),
    #     ("BA", "BIH"),
    #     ("BB", "BRB"),
    #     ("BD", "BGD"),
    #     ("BE", "BEL"),
    #     ("BF", "BFA"),
    #     ("BG", "BGR"),
    #     ("BH", "BHR"),
    #     ("BI", "BDI"),
    #     ("BJ", "BEN"),
    #     ("BL", "BLM"),
    #     ("BM", "BMU"),
    #     ("BN", "BRN"),
    #     ("BO", "BOL"),
    #     ("BQ", "BES"),
    #     ("BR", "BRA"),
    #     ("BS", "BHS"),
    #     ("BT", "BTN"),
    #     ("BV", "BVT"),
    #     ("BW", "BWA"),
    #     ("BY", "BLR"),
    #     ("BZ", "BLZ"),
    #     ("CA", "CAN"),
    #     ("CC", "CCK"),
    #     ("CD", "COD"),
    #     ("CF", "CAF"),
    #     ("CG", "COG"),
    #     ("CH", "CHE"),
    #     ("CI", "CIV"),
    #     ("CK", "COK"),
    #     ("CL", "CHL"),
    #     ("CM", "CMR"),
    #     ("CN", "CHN"),
    #     ("CO", "COL"),
    #     ("CR", "CRI"),
    #     ("CU", "CUB"),
    #     ("CV", "CPV"),
    #     ("CW", "CUW"),
    #     ("CX", "CXR"),
    #     ("CY", "CYP"),
    #     ("CZ", "CZE"),
    #     ("DE", "DEU"),
    #     ("DJ", "DJI"),
    #     ("DK", "DNK"),
    #     ("DM", "DMA"),
    #     ("DO", "DOM"),
    #     ("DZ", "DZA"),
    #     ("EC", "ECU"),
    #     ("EE", "EST"),
    #     ("EG", "EGY"),
    #     ("EH", "ESH"),
    #     ("ER", "ERI"),
    #     ("ES", "ESP"),
    #     ("ET", "ETH"),
    #     ("FI", "FIN"),
    #     ("FJ", "FJI"),
    #     ("FK", "FLK"),
    #     ("FM", "FSM"),
    #     ("FO", "FRO"),
    #     ("FR", "FRA"),
    #     ("GA", "GAB"),
    #     ("GB", "GBR"),
    #     ("GD", "GRD"),
    #     ("GE", "GEO"),
    #     ("GF", "GUF"),
    #     ("GG", "GGY"),
    #     ("GH", "GHA"),
    #     ("GI", "GIB"),
    #     ("GL", "GRL"),
    #     ("GM", "GMB"),
    #     ("GN", "GIN"),
    #     ("GP", "GLP"),
    #     ("GQ", "GNQ"),
    #     ("GR", "GRC"),
    #     ("GS", "SGS"),
    #     ("GT", "GTM"),
    #     ("GU", "GUM"),
    #     ("GW", "GNB"),
    #     ("GY", "GUY"),
    #     ("HK", "HKG"),
    #     ("HM", "HMD"),
    #     ("HN", "HND"),
    #     ("HR", "HRV"),
    #     ("HT", "HTI"),
    #     ("HU", "HUN"),
    #     ("ID", "IDN"),
    #     ("IE", "IRL"),
    #     ("IL", "ISR"),
    #     ("IM", "IMN"),
    #     ("IN", "IND"),
    #     ("IO", "IOT"),
    #     ("IQ", "IRQ"),
    #     ("IR", "IRN"),
    #     ("IS", "ISL"),
    #     ("IT", "ITA"),
    #     ("JE", "JEY"),
    #     ("JM", "JAM"),
    #     ("JO", "JOR"),
    #     ("JP", "JPN"),
    #     ("KE", "KEN"),
    #     ("KG", "KGZ"),
    #     ("KH", "KHM"),
    #     ("KI", "KIR"),
    #     ("KM", "COM"),
    #     ("KN", "KNA"),
    #     ("KP", "PRK"),
    #     ("KR", "KOR"),
    #     ("KW", "KWT"),
    #     ("KY", "CYM"),
    #     ("KZ", "KAZ"),
    #     ("LA", "LAO"),
    #     ("LB", "LBN"),
    #     ("LC", "LCA"),
    #     ("LI", "LIE"),
    #     ("LK", "LKA"),
    #     ("LR", "LBR"),
    #     ("LS", "LSO"),
    #     ("LT", "LTU"),
    #     ("LU", "LUX"),
    #     ("LV", "LVA"),
    #     ("LY", "LBY"),
    #     ("MA", "MAR"),
    #     ("MC", "MCO"),
    #     ("MD", "MDA"),
    #     ("ME", "MNE"),
    #     ("MF", "MAF"),
    #     ("MG", "MDG"),
    #     ("MH", "MHL"),
    #     ("MK", "MKD"),
    #     ("ML", "MLI"),
    #     ("MM", "MMR"),
    #     ("MN", "MNG"),
    #     ("MO", "MAC"),
    #     ("MP", "MNP"),
    #     ("MQ", "MTQ"),
    #     ("MR", "MRT"),
    #     ("MS", "MSR"),
    #     ("MT", "MLT"),
    #     ("MU", "MUS"),
    #     ("MV", "MDV"),
    #     ("MW", "MWI"),
    #     ("MX", "MEX"),
    #     ("MY", "MYS"),
    #     ("MZ", "MOZ"),
    #     ("NA", "NAM"),
    #     ("NC", "NCL"),
    #     ("NE", "NER"),
    #     ("NF", "NFK"),
    #     ("NG", "NGA"),
    #     ("NI", "NIC"),
    #     ("NL", "NLD"),
    #     ("NO", "NOR"),
    #     ("NP", "NPL"),
    #     ("NR", "NRU"),
    #     ("NU", "NIU"),
    #     ("NZ", "NZL"),
    #     ("OM", "OMN"),
    #     ("PA", "PAN"),
    #     ("PE", "PER"),
    #     ("PF", "PYF"),
    #     ("PG", "PNG"),
    #     ("PH", "PHL"),
    #     ("PK", "PAK"),
    #     ("PL", "POL"),
    #     ("PM", "SPM"),
    #     ("PN", "PCN"),
    #     ("PR", "PRI"),
    #     ("PS", "PSE"),
    #     ("PT", "PRT"),
    #     ("PW", "PLW"),
    #     ("PY", "PRY"),
    #     ("QA", "QAT"),
    #     ("RE", "REU"),
    #     ("RO", "ROU"),
    #     ("RS", "SRB"),
    #     ("RU", "RUS"),
    #     ("RW", "RWA"),
    #     ("SA", "SAU"),
    #     ("SB", "SLB"),
    #     ("SC", "SYC"),
    #     ("SD", "SDN"),
    #     ("SE", "SWE"),
    #     ("SG", "SGP"),
    #     ("SH", "SHN"),
    #     ("SI", "SVN"),
    #     ("SJ", "SJM"),
    #     ("SK", "SVK"),
    #     ("SL", "SLE"),
    #     ("SM", "SMR"),
    #     ("SN", "SEN"),
    #     ("SO", "SOM"),
    #     ("SR", "SUR"),
    #     ("SS", "SSD"),
    #     ("ST", "STP"),
    #     ("SV", "SLV"),
    #     ("SX", "SXM"),
    #     ("SY", "SYR"),
    #     ("SZ", "SWZ"),
    #     ("TC", "TCA"),
    #     ("TD", "TCD"),
    #     ("TF", "ATF"),
    #     ("TG", "TGO"),
    #     ("TH", "THA"),
    #     ("TJ", "TJK"),
    #     ("TK", "TKL"),
    #     ("TL", "TLS"),
    #     ("TM", "TKM"),
    #     ("TN", "TUN"),
    #     ("TO", "TON"),
    #     ("TR", "TUR"),
    #     ("TT", "TTO"),
    #     ("TV", "TUV"),
    #     ("TW", "TWN"),
    #     ("TZ", "TZA"),
    #     ("UA", "UKR"),
    #     ("UG", "UGA"),
    #     ("UM", "UMI"),
    #     ("US", "USA"),
    #     ("UY", "URY"),
    #     ("UZ", "UZB"),
    #     ("VA", "VAT"),
    #     ("VC", "VCT"),
    #     ("VE", "VEN"),
    #     ("VG", "VGB"),
    #     ("VI", "VIR"),
    #     ("VN", "VNM"),
    #     ("VU", "VUT"),
    #     ("WF", "WLF"),
    #     ("WS", "WSM"),
    #     ("XK", "Not provided"),
    #     ("YE", "YEM"),
    #     ("YT", "MYT"),
    #     ("ZA", "ZAF"),
    #     ("ZM", "ZMB"),
    #     ("ZW", "ZWE")]))
    #
    from pyspark.sql import types as t
    def countryCode(x):
        new_dict = {
            "AD": "AND",
            "AE": "ARE",
            "AF": "AFG",
            "AG": "ATG",
            "AI": "AIA",
            "AL": "ALB",
            "AM": "ARM",
            "AO": "AGO",
            "AQ": "ATA",
            "AR": "ARG",
            "AS": "ASM",
            "AT": "AUT",
            "AU": "AUS",
            "AW": "ABW",
            "AX": "ALA",
            "AZ": "AZE",
            "BA": "BIH",
            "BB": "BRB",
            "BD": "BGD",
            "BE": "BEL",
            "BF": "BFA",
            "BG": "BGR",
            "BH": "BHR",
            "BI": "BDI",
            "BJ": "BEN",
            "BL": "BLM",
            "BM": "BMU",
            "BN": "BRN",
            "BO": "BOL",
            "BQ": "BES",
            "BR": "BRA",
            "BS": "BHS",
            "BT": "BTN",
            "BV": "BVT",
            "BW": "BWA",
            "BY": "BLR",
            "BZ": "BLZ",
            "CA": "CAN",
            "CC": "CCK",
            "CD": "COD",
            "CF": "CAF",
            "CG": "COG",
            "CH": "CHE",
            "CI": "CIV",
            "CK": "COK",
            "CL": "CHL",
            "CM": "CMR",
            "CN": "CHN",
            "CO": "COL",
            "CR": "CRI",
            "CU": "CUB",
            "CV": "CPV",
            "CW": "CUW",
            "CX": "CXR",
            "CY": "CYP",
            "CZ": "CZE",
            "DE": "DEU",
            "DJ": "DJI",
            "DK": "DNK",
            "DM": "DMA",
            "DO": "DOM",
            "DZ": "DZA",
            "EC": "ECU",
            "EE": "EST",
            "EG": "EGY",
            "EH": "ESH",
            "ER": "ERI",
            "ES": "ESP",
            "ET": "ETH",
            "FI": "FIN",
            "FJ": "FJI",
            "FK": "FLK",
            "FM": "FSM",
            "FO": "FRO",
            "FR": "FRA",
            "GA": "GAB",
            "GB": "GBR",
            "GD": "GRD",
            "GE": "GEO",
            "GF": "GUF",
            "GG": "GGY",
            "GH": "GHA",
            "GI": "GIB",
            "GL": "GRL",
            "GM": "GMB",
            "GN": "GIN",
            "GP": "GLP",
            "GQ": "GNQ",
            "GR": "GRC",
            "GS": "SGS",
            "GT": "GTM",
            "GU": "GUM",
            "GW": "GNB",
            "GY": "GUY",
            "HK": "HKG",
            "HM": "HMD",
            "HN": "HND",
            "HR": "HRV",
            "HT": "HTI",
            "HU": "HUN",
            "ID": "IDN",
            "IE": "IRL",
            "IL": "ISR",
            "IM": "IMN",
            "IN": "IND",
            "IO": "IOT",
            "IQ": "IRQ",
            "IR": "IRN",
            "IS": "ISL",
            "IT": "ITA",
            "JE": "JEY",
            "JM": "JAM",
            "JO": "JOR",
            "JP": "JPN",
            "KE": "KEN",
            "KG": "KGZ",
            "KH": "KHM",
            "KI": "KIR",
            "KM": "COM",
            "KN": "KNA",
            "KP": "PRK",
            "KR": "KOR",
            "KW": "KWT",
            "KY": "CYM",
            "KZ": "KAZ",
            "LA": "LAO",
            "LB": "LBN",
            "LC": "LCA",
            "LI": "LIE",
            "LK": "LKA",
            "LR": "LBR",
            "LS": "LSO",
            "LT": "LTU",
            "LU": "LUX",
            "LV": "LVA",
            "LY": "LBY",
            "MA": "MAR",
            "MC": "MCO",
            "MD": "MDA",
            "ME": "MNE",
            "MF": "MAF",
            "MG": "MDG",
            "MH": "MHL",
            "MK": "MKD",
            "ML": "MLI",
            "MM": "MMR",
            "MN": "MNG",
            "MO": "MAC",
            "MP": "MNP",
            "MQ": "MTQ",
            "MR": "MRT",
            "MS": "MSR",
            "MT": "MLT",
            "MU": "MUS",
            "MV": "MDV",
            "MW": "MWI",
            "MX": "MEX",
            "MY": "MYS",
            "MZ": "MOZ",
            "NA": "NAM",
            "NC": "NCL",
            "NE": "NER",
            "NF": "NFK",
            "NG": "NGA",
            "NI": "NIC",
            "NL": "NLD",
            "NO": "NOR",
            "NP": "NPL",
            "NR": "NRU",
            "NU": "NIU",
            "NZ": "NZL",
            "OM": "OMN",
            "PA": "PAN",
            "PE": "PER",
            "PF": "PYF",
            "PG": "PNG",
            "PH": "PHL",
            "PK": "PAK",
            "PL": "POL",
            "PM": "SPM",
            "PN": "PCN",
            "PR": "PRI",
            "PS": "PSE",
            "PT": "PRT",
            "PW": "PLW",
            "PY": "PRY",
            "QA": "QAT",
            "RE": "REU",
            "RO": "ROU",
            "RS": "SRB",
            "RU": "RUS",
            "RW": "RWA",
            "SA": "SAU",
            "SB": "SLB",
            "SC": "SYC",
            "SD": "SDN",
            "SE": "SWE",
            "SG": "SGP",
            "SH": "SHN",
            "SI": "SVN",
            "SJ": "SJM",
            "SK": "SVK",
            "SL": "SLE",
            "SM": "SMR",
            "SN": "SEN",
            "SO": "SOM",
            "SR": "SUR",
            "SS": "SSD",
            "ST": "STP",
            "SV": "SLV",
            "SX": "SXM",
            "SY": "SYR",
            "SZ": "SWZ",
            "TC": "TCA",
            "TD": "TCD",
            "TF": "ATF",
            "TG": "TGO",
            "TH": "THA",
            "TJ": "TJK",
            "TK": "TKL",
            "TL": "TLS",
            "TM": "TKM",
            "TN": "TUN",
            "TO": "TON",
            "TR": "TUR",
            "TT": "TTO",
            "TV": "TUV",
            "TW": "TWN",
            "TZ": "TZA",
            "UA": "UKR",
            "UG": "UGA",
            "UM": "UMI",
            "US": "USA",
            "UY": "URY",
            "UZ": "UZB",
            "VA": "VAT",
            "VC": "VCT",
            "VE": "VEN",
            "VG": "VGB",
            "VI": "VIR",
            "VN": "VNM",
            "VU": "VUT",
            "WF": "WLF",
            "WS": "WSM",
            "XK": "Not provided",
            "YE": "YEM",
            "YT": "MYT",
            "ZA": "ZAF",
            "ZM": "ZMB",
            "ZW": "ZWE"
        }
        return new_dict[x]

    countrycodeUDF = F.udf(countryCode, t.StringType())
    s_payee_df = sample_tt_df(spark_session_)
    s_payee_df \
        .withColumn('s_beneficiary_bank_country_code1',
                    F.regexp_replace(F.col("beneficiarys_bank_account_number"), "[0-9a-zA-Z-]", "X")) \
        .withColumn('beneficiarys_country_code_new', countrycodeUDF(F.col("beneficiarys_country_code"))) \
        .show(truncate=False)

    '#  country_code_UDF(F.col(Schema.s_beneficiary_country_code)'


def test_subtract_dataframe(_framework_context, spark_session_):
    data_prev = [("c-1", "P-1", "B1", "Y"),
                 ("c-2", "P-2", "B2", "Y"),
                 ("c-3", "P-3", "B3", "Y"),
                 ("c-4", "P-4", "B4", "Y"),
                 ("c-5", "P-5-1", "B5", "Y"),
                 ("c-5", "P-5-2", "b51", "Y"),
                 ("c-7", "P-7-1", "B71", "Y"),
                 ("c-7", "P-7-2", "B72", "Y"),
                 ("c-8", "P-8-2", "B8", "Y")
                 ]

    data_curr = [("c-1", "P-1", "B1", "Y"),
                 ("c-2", "P-2", "B2", "Y"),
                 ("c-3", "P-3", "B3", "Y"),
                 ("c-4", "P-4", "B4", "Y"),
                 ("c-5", "P-5-2", "b52", "Y")
                 ]
    schema = StructType([
        StructField("cis", StringType(), True),
        StructField("payee", StringType(), True),
        StructField("odate", StringType(), True)
    ])

    prev_df = spark_session_ \
        .createDataFrame(data=data_prev, schema=["cis", "payee", "biller", "isactive"]) \
        .withColumn("upddt", F.from_unixtime(F.unix_timestamp(F.lit("2021-03-07"), 'yyyy-MM-dd')).cast(TimestampType()))
    prev_df.show()
    curr_df = spark_session_ \
        .createDataFrame(data=data_curr, schema=["cis", "payee", "biller", "isactive"]) \
        .withColumn("upddt", F.from_unixtime(F.unix_timestamp(F.lit("2021-03-08"), 'yyyy-MM-dd')).cast(TimestampType()))
    curr_df.show()

    joined_df = curr_df.union(prev_df)

    window_function = Window.partitionBy(["cis", "payee"]).orderBy(F.col("upddt").desc())

    without_dup_df = joined_df.withColumn('rank', F.row_number().over(window_function))

    without_dup_df.show(100, False)

    without_dup_df = without_dup_df.filter(F.col('rank') == 1).withColumn("isactive", F.when(without_dup_df["upddt"] < "2021-03-08", False)
                                               .otherwise(True))
    without_dup_df.show(100, False)


def test_check_dfs_location(_framework_context, spark_session_):

    conf =  spark_session_.sparkContext._jsc.hadoopConfiguration()
    fs = spark_session_.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(conf)
    exists = fs.exists(spark_session_.sparkContext._jvm.org.apache.hadoop.fs.Path("/tmp/data/a001fc_acct_detl_lending_deposit_rates_reference"))

    print(exists)
