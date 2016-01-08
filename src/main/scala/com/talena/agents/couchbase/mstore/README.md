## Scala Notes
#### Option Class
Option is used whenever applicable. This gives strong type safety and checking and eliminates all NPEs for good. Option also supports map(), filter() and other useful methods to make error handling easy and intuitive.

http://twitter.github.io/effectivescala/#Functional programming-Options.
http://www.scala-lang.org/api/current/#scala.Option

#### Apply() method
The Scala apply() method is used in a number of places. This allows for some nice syntactic sugar.

https://twitter.github.io/scala_school/basics2.html#apply

#### Sealed Traits/AbstractClasses and Case Classes
Sealed traits and abstract classes, and case classes are used to model Algebraic Data Types (ADTs). Marking something as "sealed" allows the compiler to do an exhaustive type check of all alternatives of a parent trait/class during pattern matching. Thus, if we do not handle an alternative in the case clause, the compiler will warn us.

http://docs.scala-lang.org/tutorials/tour/case-classes.html

#### Companion Objects
Companion objects are extensively used. Companion objects share the same name as the class for which they are a companion and both have complete access to each other's members and methods. Companion objects typically hold all of a class' static methods including any factory methods
to create instances of the class or any of its subclasses.

http://docs.scala-lang.org/tutorials/tour/singleton-objects.html

#### For Comprehensions
Scala's for-comprehensions are used to iterate over filter and mutations tuples of a Spark partition during deduplication, compaction and mapping operations.

http://alvinalexander.com/scala/scala-for-loop-yield-examples-yield-tutorial
