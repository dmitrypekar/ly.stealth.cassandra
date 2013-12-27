package ly.stealth.cassandra

import org.junit.{After, Before}
import com.datastax.driver.core.Cluster
import ly.stealth.cassandra.Db

abstract class DbTest {
  @Before
  def createTable() {
    val cluster = Cluster.builder()
      .addContactPoints("localhost")
      .build()

    Db.init(cluster, "test")
    ItemTable.drop()
    ItemTable.create()
  }

  @After
  def dropTable() {
    ItemTable.drop()
    Db.shutdown()
  }
}
