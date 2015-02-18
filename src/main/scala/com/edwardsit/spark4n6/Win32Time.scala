import java.nio.ByteBuffer
import org.joda.time.DateTime
import org.joda.time.DateTimeZone

object Win32Time {
  def fromFILETIME(a: Array[Byte]): DateTime = {
    val dt = ByteBuffer.wrap(a.reverse).getLong
    return new DateTime((dt.toLong - 116444736000000000L) / 10000L, DateTimeZone.UTC)
  }
}
