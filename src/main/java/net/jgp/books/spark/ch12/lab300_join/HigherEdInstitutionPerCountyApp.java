package net.jgp.books.spark.ch12.lab300_join;

import static org.apache.spark.sql.functions.element_at;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.split;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Performs a join between 3 datasets to build a list of higher education
 * institutions per county.
 *
 * @author jgp
 */
public class HigherEdInstitutionPerCountyApp {

    /**
     * main() is your entry point to the application.
     *
     * @param args
     */
    public static void main(String[] args) {
        HigherEdInstitutionPerCountyApp app =
                new HigherEdInstitutionPerCountyApp();
        app.start();
    }

    /**
     * The processing code.
     */
    private void start() {
        // Creation of the session
        SparkSession spark = SparkSession.builder()
                .appName("Join")
                .master("local")
                .getOrCreate();

        // Ingestion of the census data
        Dataset<Row> censusDf = spark
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("encoding", "cp1252") // otherwise UTF-8 would be assumed
                .load("data/census/PEP_2017_PEPANNRES.csv");
        censusDf = censusDf
                .drop("GEO.id")
                .drop("rescen42010")
                .drop("resbase42010")
                .drop("respop72010")
                .drop("respop72011")
                .drop("respop72012")
                .drop("respop72013")
                .drop("respop72014")
                .drop("respop72015")
                .drop("respop72016")
                .withColumnRenamed("respop72017", "pop2017")
                .withColumnRenamed("GEO.id2", "countyId")
                .withColumnRenamed("GEO.display-label", "county");
        System.out.println("Census data");
        censusDf.sample(0.1).show(3, false);
        censusDf.printSchema();

        // Higher education institution (and yes, there is an Arkansas College
        // of Barbering and Hair Design)
        Dataset<Row> higherEdDf = spark
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("data/dapip/InstitutionCampus.csv");
        higherEdDf = higherEdDf
                .filter("LocationType = 'Institution'")
                // split up the address delimited by spaces to isolate the zip code
                .withColumn(
                        "addressElements",
                        split(higherEdDf.col("Address"), " "));
        higherEdDf = higherEdDf
                // find the number of address elements (differs per record) to select the last element as zip code
                .withColumn(
                        "addressElementCount",
                        size(higherEdDf.col("addressElements")));
        higherEdDf = higherEdDf
                // select the zip code as the last address element (element_at uses the 1 based SQL array)
                .withColumn(
                        "zip9",
                        element_at(
                                higherEdDf.col("addressElements"),
                                higherEdDf.col("addressElementCount"))); // (1 based, the first index is 1 not 0)
        higherEdDf = higherEdDf
                // some zip codes have the + 4 extension separated with a dash
                // the column "splitZipCode" will contain an array with other 1 or 2 elements
                .withColumn(
                        "splitZipCode",
                        split(higherEdDf.col("zip9"), "-"));
        higherEdDf = higherEdDf
                // we will select the first element of "splitZipCode" (which will always be present)
                .withColumn("zip", higherEdDf.col("splitZipCode").getItem(0))
                .withColumnRenamed("LocationName", "location")
                .drop("DapipId")
                .drop("OpeId")
                .drop("ParentName")
                .drop("ParentDapipId")
                .drop("LocationType")
                .drop("Address")
                .drop("GeneralPhone")
                .drop("AdminName")
                .drop("AdminPhone")
                .drop("AdminEmail")
                .drop("Fax")
                .drop("UpdateDate")
                .drop("zip9")
                .drop("addressElements")
                .drop("addressElementCount")
                .drop("splitZipCode");
        System.out.println("Higher education institutions (DAPIP)");
        higherEdDf.sample(0.1).show(3, false);
        higherEdDf.printSchema();

        // Zip to county
        Dataset<Row> countyZipDf = spark
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("data/hud/COUNTY_ZIP_092018.csv");
        countyZipDf = countyZipDf
                .drop("res_ratio")
                .drop("bus_ratio")
                .drop("oth_ratio")
                .drop("tot_ratio");
        System.out.println("Counties / ZIP Codes (HUD)");
        countyZipDf.sample(0.1).show(3, false);
        countyZipDf.printSchema();

        // Institutions per county id by inner joining the zip columns of both dataframes
        // we end up with 2 zip columns
        Dataset<Row> institPerCountyDf = higherEdDf.join(
                countyZipDf,
                higherEdDf.col("zip").equalTo(countyZipDf.col("zip")),
                "inner");
        System.out.println(
                "Higher education institutions inner joined on zip with HUD");
        institPerCountyDf
                .filter(higherEdDf.col("zip").equalTo(27517))
                .show(20, false);
        institPerCountyDf.printSchema();

        // --------------------------
        // - "Temporary" drop columns

        // Note:
        // This block is not doing anything except illustrating that the drop()
        // method needs to be used carefully.

        // Dropping all zip columns, this would be unintentional we want to drop only 1 of the 2 zip columns
        System.out.println(
                "Attempt to drop one of the zip column unintentionally dropping both");
        institPerCountyDf.drop("zip").sample(0.1).show(3, false);

        // Dropping one specific zip columns by specifying the one inherited from the higher ed dataframe
        System.out.println(
                "Dropping one of the zip columns by specifying the dataframe from which it originated");
        institPerCountyDf
                .drop(higherEdDf.col("zip"))
                .sample(0.1).show(3, false);
        // --------------------------

        // Institutions per county name
        // doing a left join on county id with the censusDf
        institPerCountyDf = institPerCountyDf.join(
                censusDf,
                institPerCountyDf.col("county").equalTo(censusDf.col("countyId")),
                "left");

        // Final clean up dropping columns that are no longer necessary after the join operation
        institPerCountyDf = institPerCountyDf
                .drop(higherEdDf.col("zip"))
                .drop(countyZipDf.col("county"))
                .drop("countyId")
                .distinct(); //removing duplicate records

        System.out.println(
                "Higher education institutions in ZIP Code 27517 (NC)");
        institPerCountyDf
                .filter(higherEdDf.col("zip").equalTo(27517))
                .show(20, false);

        System.out.println(
                "Higher education institutions in ZIP Code 02138 (MA)");
        institPerCountyDf
                .filter(higherEdDf.col("zip").equalTo(2138))
                .show(20, false);

        System.out.println("Institutions with improper counties");
        institPerCountyDf.filter("county is null").show(200, false);

        System.out.println("Final list");
        institPerCountyDf.show(200, false);
        System.out.println("The combined list has " + institPerCountyDf.count()
                + " elements.");

        // A little more
        // @formatter:off
        //    Dataset<Row> aggDf = institutionPerCountyDf
        //        .groupBy("county", "pop2017")
        //        .count();
        //    aggDf = aggDf.orderBy(aggDf.col("count").desc());
        //    aggDf.show(25, false);
        //
        //    Dataset<Row> popDf = aggDf
        //        .filter("pop2017>30000")
        //        .withColumn("institutionPer10k", expr("count*10000/pop2017"));
        //    popDf = popDf.orderBy(popDf.col("institutionPer10k").desc());
        //    popDf.show(25, false);
        // @formatter:on
    }
}
