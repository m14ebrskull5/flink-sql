package org.example

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api._


/**
 * CREATE TABLE t5(
 order_id INT,
 order_date TIMESTAMP(3),
 customer_name VARCHAR(20),
 price DECIMAL(10, 5),
 product_id INT,
 order_status BOOLEAN,
 PRIMARY KEY (order_id) NOT ENFORCED
 )
 PARTITIONED BY (`order_status`)
 WITH (
   'connector' = 'hudi',
   'path' = 'oss://gakef/t5',
   'table.type' = 'MERGE_ON_READ',
   'read.streaming.enabled' = 'true',  -- this option enable the streaming read
   'read.start-commit' = '20210316134557', -- specifies the start commit instant time
   'read.streaming.check-interval' = '4' -- specifies the check interval for finding new source commits, default 60s.
 );
 */
object Flink {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings
            .newInstance()
            .inStreamingMode()
            //.inBatchMode()
            .build()
    val tableEnv = TableEnvironment.create(settings)
    tableEnv.createTable("t5", TableDescriptor.forConnector("hudi")
            .schema(Schema.newBuilder()
                    .column("order_id", DataTypes.INT().notNull())
                    .column("order_date", DataTypes.TIMESTAMP(3))

                    .column("customer_name", DataTypes.INT())

                    .column("price", DataTypes.DECIMAL(10, 5))

                    .column("product_id", DataTypes.INT())
                    .column("order_status", DataTypes.BOOLEAN())
                    .primaryKey("order_id")

                    .build()
            )
            .partitionedBy("order_status")
            .option("path", "oss://gakef/t5")
            .option("table.type", "MERGE_ON_READ")
            .option("read.streaming.enabled", "true")
            .option("read.start-commit", "20210316134557")
            .option("read.streaming.check-interval", "4")
            .build())
    var t5 = tableEnv.from("t5");
    val result: TableResult = t5.fetch(2).execute()
    result.print()

  }
}
