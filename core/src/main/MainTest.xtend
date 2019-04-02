import java.security.Security
import org.bouncycastle.jce.provider.BouncyCastleProvider

class MainTest {
  def static void main(String[] args) {
    Security.addProvider(new BouncyCastleProvider)
  }
}