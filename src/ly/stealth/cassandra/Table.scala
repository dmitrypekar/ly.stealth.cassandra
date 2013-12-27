package ly.stealth.cassandra

import com.datastax.driver.core._
import com.datastax.driver.core.BatchStatement

abstract class Table[T >: Null] {
  def name: String

  def keyColumn: String

  def keyValue(t: T): Any

  def create()

  def clear()

  def drop()

  def materialize(row: Row): T

  def getSaveStatement(t: T): BoundStatement

  def saveAll(list: List[T]): List[T] = {
    val bs: BatchStatement = new BatchStatement()

    for (t <- list)
      bs.add(getSaveStatement(t))

    Db.execute(bs)
    list
  }

  def save(t: T): T = {
    Db.execute(getSaveStatement(t))
    t
  }

  def delete(t: T) = query.key(keyValue(t)).delete()

  def query: Query[T]
}


