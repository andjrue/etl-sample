# etl-sample

This repository provides a sample ETL pipeline implemented with the abstract factory pattern. This pattern allows for efficient, scalable, and flexible solutions. It enforces
a consistent structure while allowing for individual customization of each data source.

#### Overview

Each class inherits from the ETL base class and must implement the core methods (`extract_data`, `transform_data`, & `load_data`) defined under the @abstractmethod decorator. This approach ensures that:
- Every ETL pipeline follows the same fundamental structure.
- New pipelines can be developed quickly by following a consistent template.
- There is flexibility for collaborators to add or customize methods as needed.

Along with the last point, there probably won't be a real world case as simple as the one I've implemented. This is mostly a way to show that it could work, as opposed to how it actually will work.
However, it would be easy to expand the `transform_data` method. You could build functions that the method calls, thereby further simplifying your code base and sticking to SOLID principles.

#### Benefits

- Scalability: Data sources can be added or removed with minimal effort. To add a source, you simply need to create a new class that inherits the `ETL Base Class`. For example,
you could add a `PostgresETL` class to load data into a Postgres DB rather than a CSV.
- Consistency: The use of `@abstractmethod` enfores that each subclass implements the core methods listed in overview. This ensures a consistent process, even when each pipeline has unique data sources
or transformation logic.
- Flexbility & Modularity: While the `transform_data` method in this example is simple, it could be expanded to include multiple transformation steps. This modularity allows for complex transformations
while keeping the code organized and maintainable.
- Collaboration-Friendly: The clear and consistent structure makes it easy for other developers to understand and contribute to the codebase. You can also implement a CI/CD pipeline with GitHub
Actions to enfore code quality and prevent major issues beofre merging new changes.
