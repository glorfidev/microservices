package toolkit

import org.scalatest.funsuite.AnyFunSuite

class LocalAccessSpec extends AnyFunSuite {
  private val access = new LocalAccess(9090)

  test("only the console's loopback hosts are accepted") {
    assert(access.allowsHost("127.0.0.1:9090"))
    assert(access.allowsHost("localhost:9090"))
    assert(!access.allowsHost("localhost:8080"))
    assert(!access.allowsHost("example.invalid:9090"))
  }

  test("management requests need the intent header and an exact local origin") {
    assert(access.allowsMutation("localhost:9090", Some("http://localhost:9090"), Some("1")))
    assert(access.allowsMutation("localhost:9090", None, Some("1")))
    assert(!access.allowsMutation("localhost:9090", Some("http://127.0.0.1:9090"), Some("1")))
    assert(!access.allowsMutation("localhost:9090", Some("http://example.invalid"), Some("1")))
    assert(!access.allowsMutation("localhost:9090", Some("http://localhost:9090"), None))
  }
}
