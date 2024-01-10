package el.asmae;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class App {
    public static void main(String[] args) {
        SparkSession ss=SparkSession.builder().appName("TP SPARK SQL").master("spark://spark-master:7077").getOrCreate();
        Dataset<Row> dframe1=ss.read().option("header",true).option("inferSchema",true).csv("/bitnami/achats.csv");
        dframe1.show();
        dframe1.printSchema();

        //1. Afficher le produit le plus vendu en terme de montant total.
        dframe1.groupBy(col("produit_id")).sum("montant").orderBy(col("sum(montant)").desc()).limit(1).show();
        //2. Afficher les 3 produits les plus vendus dans l'ensemble des données.
        dframe1.groupBy(col("produit_id")).sum("montant").orderBy(col("sum(montant)").desc()).limit(3).show();
        //3. Afficher le montant total des achats pour chaque produit.
        dframe1.groupBy(col("produit_id")).sum("montant").show();
    }
}
