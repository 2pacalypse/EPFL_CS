# Deliberative Homework
## States and Actions
The state is defined as :
* The position of the vehicle
* The carried tasks by the vehicle
* The available tacks in the environement 

This choice of state work well with the possible action offered by the Logist Platform
The three types of action are :
* Move
* Pickup task
* Deliver task

They all led to a new state by only changing one parameter of it

Two state are defined equals if they have the same position, carried task and availeble task.
They can have differents history (parent and action that led to it) and still be equals.

They can be compared by their value of *f()* 

