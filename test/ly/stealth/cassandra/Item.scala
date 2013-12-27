package ly.stealth.cassandra

import com.datastax.driver.core.{BoundStatement, PreparedStatement, Session, Row}
import java.util.UUID
import scala.collection.mutable.ListBuffer
import ly.stealth.cassandra.{Table, Query, Db}

class Item(var name: String = "") {
  var id: String = "" + UUID.randomUUID()

  def save() = {
    ItemTable.save(this)
    this
  }

  def delete() = ItemTable.delete(this)

  override def equals(obj: scala.Any): Boolean =
    obj.isInstanceOf[Item] && id == obj.asInstanceOf[Item].id
}

object Item {
  def query() = ItemTable.query

  def byId(id: String) = query().key(id).first()

  def saveAll(items: List[Item]) = ItemTable.saveAll(items)
}

class ItemQuery extends Query[Item](ItemTable) {
  private var _name: String = null

  def name(name: String) = { _name = name; this }
  def name() = _name

  override def conditions: List[String] = {
    val conditions = new ListBuffer[String]()

    if (_name != null)
      conditions += "name='" + Db.stringLiteral(_name) + "'"

    conditions.toList
  }
}

object ItemTable extends Table[Item] {
  def name: String = "Item"

  def keyColumn: String = "id"

  def keyValue(item: Item): String = item.id

  def create(): Unit = {
    Db.execute(s"create table $name (id varchar primary key, name varchar)")
    Db.execute(s"create index Item_name on $name(name)")
  }

  def clear(): Unit = Db.execute(s"delete from table $name")

  def drop(): Unit = Db.execute(s"drop table if exists $name")

  def materialize(row: Row):Item = {
    val item: Item = new Item()
    item.name = row.getString("name")
    item.id = row.getString("id")
    item
  }

  def getSaveStatement(item: Item): BoundStatement = {
    val cql = s"insert into $name (id, name) values (?, ?)"
    val ps: PreparedStatement = Db.session().prepare(cql)
    ps.bind(item.id, item.name)
  }

  def query: ItemQuery = new ItemQuery()
}
