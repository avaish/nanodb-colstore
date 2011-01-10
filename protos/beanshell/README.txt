This experiment is for verifying whether BeanShell scripts can contain
variables and expressions that are loosely typed and "late bound."  The
test includes a base type that defines common fields, and then subtypes
that define their own fields that differ either in name or in type across
the subtypes.  Then, BeanShell code runs against this code that will only
execute if BeanShell can do late-bound evaluations.

Result:  It works!  BeanShell can do late-bound expression evaluations.

To Run:

    javac *.java
    java -cp .;bsh-2.0b4.jar bsh.Interpreter test.bsh

(Update classpath to use ":" instead of ";" on MacOS/X or on Linux.)

