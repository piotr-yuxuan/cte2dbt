## Object-oriented Refactoring

An abstract class =TableTransformer= should have two child classes for
source and CTE that contain logic specific to them. Instances of these
classes should be created by a =TableTransformerFactory=.

These table transformers should be used within another class that
extracts models, =cte_names=, =source_names= and returns immutable
data model instances.
