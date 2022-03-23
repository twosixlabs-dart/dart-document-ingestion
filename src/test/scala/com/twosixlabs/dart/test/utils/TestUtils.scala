package com.twosixlabs.dart.test.utils

import scala.util.Random

object TestUtils {

    def randomPort( min : Int = 2000, max : Int = 20000 ) : Int = min + Random.nextInt( ( max - min ) + 1 )

}
