package ly.stealth.cassandra

import org.junit.Test
import org.junit.Assert._

// This test serves as example of exposing db API to app class
class ItemTest extends DbTest {
  @Test
  def save_byId() {
    val item = new Item().save()
    assertEquals(item, Item.byId(item.id))
  }

  @Test
  def saveAll() {
    Item.saveAll(List(new Item(), new Item(), new Item()))
    assertEquals(3, Item.query().total())
  }

  @Test
  def delete() {
    val item = new Item().save()
    assertEquals(item, Item.byId(item.id))

    item.delete()
    assertNull(Item.byId(item.id))
  }
}
