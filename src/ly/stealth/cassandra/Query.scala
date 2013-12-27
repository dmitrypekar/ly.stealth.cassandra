package ly.stealth.cassandra

import java.lang.String
import com.datastax.driver.core.{Row, ResultSet}
import java.util
import scala.collection.mutable.ListBuffer

class Query[T >: Null](table: Table[T]) {
  private var _keys: List[Any] = null

  def keys(): List[Any] = _keys

  def key(key: Any) = keys(if (key != null) List(key) else null)
  def keys(keys: List[Any]): Query[T] = {
    this._keys = keys
    this
  }

  def whereClause = {
    val criteria = new ListBuffer[String]()

    if (_keys != null)
      if (_keys.isEmpty)
        criteria += "0=1"
      else if (_keys.length == 1)
        criteria += table.keyColumn + "=" + Db.literal(_keys(0))
      else {
        var in = ""

        for (key <- _keys) {
          if (!in.isEmpty) in += ","
          in += Db.literal(key)
        }

        criteria += table.keyColumn + " in (" + in + ")"
      }

    criteria ++= conditions

    if (!criteria.isEmpty) "where " + criteria.mkString(" and ") else ""
  }

  def conditions: List[String] = List()
  def orderClause = ""

  def first(): T = {
    val list = this.list(0, 1)
    if (list.isEmpty) null else list(0)
  }

  def list(start: Int = 0, count: Int = -1): List[T] = {
    if (count == 0) return List()
    val limit = if (count >= 0) start + count else -1

    var cql: String = s"select * from " + table.name
    cql += " " + whereClause
    cql += " " + orderClause
    if (limit != -1) cql += " limit " + limit

    val rs = Db.execute(cql)
    val rows: util.List[Row] = rs.all()
    val slice = rows.subList(Math.min(start, rows.size()), rows.size())

    import scala.collection.JavaConversions.asScalaBuffer
    val result = new ListBuffer[T]
    for (row: Row <- slice)
      result.append(table.materialize(row))

    result.toList
  }

  def total(): Long = {
    var cql: String = "select count(*) from " + table.name
    cql += " " + whereClause

    val rs: ResultSet = Db.execute(cql)
    rs.one().getLong(0)
  }

  def delete() {
    var cql: String = "delete from " + table.name
    cql += " " + whereClause

    Db.execute(cql)
  }
}
