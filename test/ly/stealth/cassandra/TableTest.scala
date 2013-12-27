package ly.stealth.cassandra

import org.junit.Test
import org.junit.Assert._

class TableTest extends DbTest {
  @Test
  def save() {
    val item = new Item("name")
    ItemTable.save(item)

    val read = Item.byId(item.id)
    assertEquals(item, read)
  }

  @Test
  def saveAll() {
    val items = List(new Item("name0"), new Item("name1"), new Item("name2"))
    ItemTable.saveAll(items)
    assertEquals(3, ItemTable.query.total())
  }
}
