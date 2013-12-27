package ly.stealth.cassandra

import com.datastax.driver.core.{Statement, ResultSet, Session, Cluster}

object Db {
  private var _cluster:Cluster = null
  private var _session:Session = null

  def init(cluster:Cluster, keySpace: String) {
    init(cluster, cluster.connect(keySpace))
  }

  def init(cluster:Cluster, session:Session) {
    _cluster = cluster
    _session = session
  }

  def shutdown() = _cluster.shutdown()

  def cluster():Cluster = _cluster
  def session():Session = _session

  def execute(cql: String): ResultSet = session().execute(cql)
  def execute(st: Statement): ResultSet = session().execute(st)

  def literal(o: Any): String = {
    o match {
      case s: String => "'" + stringLiteral(s) + "'"
      case o: Any => "" + o
    }
  }
  
  def stringLiteral(s: String) : String = s.replace("'", "''")
}
