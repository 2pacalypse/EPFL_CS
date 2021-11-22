# centralized

I havn't done much yet. But here is what i think we can do

First the three array : nextTask, time and vehicle are stored in
a class Solution from where we can create the three main
function `SelectInitialSolution()`, `ChooseNeighbours()` and `LocalChoice()`.

The main algorithm may work similarly than the one form the paper
but we must separate the pickup and delivery task to allow the vehicle
to transport different tasks.
So I've created a `TypedTask`that add a boolean to a normal Task
to indicate it type (pickup or delivery).
I was not able to use inheritance, beut it could be a better idea here.
