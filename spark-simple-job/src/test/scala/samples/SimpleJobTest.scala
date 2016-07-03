package samples

import org.junit._
import Assert._
import me.aliprax.simple_job.SimpleJob

@Test
class SimpleJobTest {

    @Test
    def testIsPrime()  {
      assertTrue(SimpleJob.isPrime(3))
    }
    
    @Test
    def testIsNotPrime {
      assertFalse(SimpleJob.isPrime(4))
    }

}


