package ly.stealth.cassandra

import org.junit.Test
import org.junit.Assert._
import ly.stealth.cassandra.Query

class QueryTest extends DbTest {
  @Test
  def list() {
    new Item("item0").save()
    new Item("item1").save()
    new Item("item2").save()

    val query: Query[Item] = Item.query()
    val items = query.list()

    for (start <- 0 to 2)
      for (count <- 0 to 3) {
        val list = query.list(start, count)
        assertEquals(s"start=$start, count=$count", items.slice(start, start + count).toList, list)
      }
  }

  @Test
  def total() {
    val item0: Item = new Item("item0").save()
    val item1: Item = new Item("item1").save()
    val item2: Item = new Item("item2").save()

    val query = Item.query()
    assertEquals(3, query.total())

    query.keys(List(item0.id, item2.id))
    assertEquals(2, query.total())

    query.key(item1.id)
    assertEquals(1, query.total())
  }

  @Test
  def total_filterByName() {
    new Item("name0").save()
    new Item("name1").save()
    new Item("name0").save()

    val query = Item.query()
    assertEquals(3, query.total())

    query.name("name0")
    assertEquals(2, query.total())

    query.name("name1")
    assertEquals(1, query.total())
  }

  @Test
  def delete() {
    val item0: Item = new Item("item0").save()
    val item1: Item = new Item("item1").save()

    val query = Item.query()
    assertEquals(2, query.total())

    query.key(item0.id).delete()

    query.key(null)
    assertEquals(List(item1), query.list())
  }
}
