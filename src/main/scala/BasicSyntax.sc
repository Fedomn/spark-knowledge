val hello: String = "Hello"

var helloThere: String = hello
helloThere += "Three!"
println(helloThere)

// data types
val num: Int = 1
val bool: Boolean = true
val letterA: Char = 'a'
val d: Double = 3.14
val f: Float = 3.1f
val bigNum: Long = 123456789
val b: Byte = 127

println(num, bool, letterA, d, f, bigNum, b)

// flow control

if (true) println(true) else println(false)

val num = 3
num match {
  case 1 => println("one")
  case 3 => println("three")
  case _ => println("something else")
}

for (x <- 1 to 4) {
  print(x)
}

// expressions
println({
  val x = 10
  x + 20
})

// functions
def squareIt(x: Int): Int = {
  x * x
}

def cubeIt(x: Int): Int = {
  x * x * x
}
println(squareIt(2), cubeIt(2))

def transformInt(x: Int, f: Int => Int): Int = {
  f(x)
}

transformInt(2, cubeIt)
transformInt(2, x => x * x * x)
transformInt(2, x => {
  val y = x * 2
  y * y
})

// data structures
// tuples immutable lists
val stuff = ("A", 1, true)
println(stuff._2)

// list
val numList = List(1, 2, 3)
println(numList(1), numList.head, numList.tail)
for (num <- numList) print(num)

val mrRes = numList
  .filter(_ != 1) // underscore is a placeholder for every element of the list
  .map(x => x * 2)
  .reduce((x: Int, y: Int) => x * y + 1)

// concatenate list
val res = List(1) ++ List(2) ++ List(3)
val reversedRes = res.reverse
val sortedRes = res.sorted
val distinctRes = (res ++ res).distinct
val maxRes = res.max

// maps
val strMap = Map("1" -> 1, "2" -> 2, "3" -> 3)
println(strMap("1"))
println(util.Try(strMap("unknown")) getOrElse "unknown")
