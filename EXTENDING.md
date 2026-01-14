## Extending the pipeline to N Jobs
The scalable design makes it easy to add more jobs:
```python
from prefect_pipeline.pipeline import PipelineDefinition, PipelineRunner
from my_jobs import ValidateJob, TransformJob, EnrichJob, ExportJob

def create_custom_pipeline() -> PipelineDefinition:
    pipeline = PipelineDefinition(name="my_pipeline")
    
    # Add jobs in execution order
    pipeline.add_job(
        name="validate",
        job_class=ValidateJob,
        config_factory=lambda: {"schema": "my_schema.json"},
        description="Validate input data",
    )
    
    pipeline.add_job(
        name="transform",
        job_class=TransformJob,
        config_factory=lambda: {"rules": transform_rules},
        description="Apply transformations",
    )
    
    pipeline.add_job(
        name="enrich",
        job_class=EnrichJob,
        config_factory=lambda: {"lookup_db": "lookups.db"},
        description="Enrich with external data",
    )
    
    pipeline.add_job(
        name="export",
        job_class=ExportJob,
        config_factory=lambda: {"destination": "/data/export"},
        description="Export final results",
    )
    
    return pipeline

# Use the custom pipeline
runner = PipelineRunner(pipeline=create_custom_pipeline())

# All the same flexible execution options work
result = runner.run(input_data)
result = runner.run(input_data, start_from="transform")
result = runner.run(input_data, stop_after="enrich")

print(runner.available_jobs)
# ['validate', 'transform', 'enrich', 'export']
```

## Creating Custom Jobs

All jobs must inherit from BaseJob and implement two methods:
```python
from pathlib import Path
from typing import Any
from prefect_pipeline.jobs import BaseJob
from prefect_pipeline.validators import ValidationError

class MyCustomJob(BaseJob[dict, list]):
    """
    Type hints: BaseJob[InputType, OutputType]
    - InputType: What single_job() receives (after validation)
    - OutputType: What single_job() returns
    """
    
    def __init__(self, config_option: str = "default") -> None:
        super().__init__()
        self.config_option = config_option
    
    def validate_input(self, input_data: Any) -> dict:
        """
        Validate and transform raw input.
        
        Raises:
            ValidationError: If input is invalid
        """
        if not isinstance(input_data, dict):
            raise ValidationError(
                f"Expected dict, got {type(input_data).__name__}",
                field="input_data"
            )
        
        if "required_key" not in input_data:
            raise ValidationError("Missing 'required_key'", field="input_data")
        
        return input_data
    
    def single_job(self, input_data: dict) -> list:
        """
        Core business logic.
        
        Args:
            input_data: Validated input (from validate_input)
            
        Returns:
            Processed output
        """
        # Your processing logic here
        results = []
        for key, value in input_data.items():
            results.append(f"{key}: {value}")
        return results

```
The execute() method is inherited from BaseJob and wraps your logic with error handling:
```python
job = MyCustomJob(config_option="custom")

# execute() handles validation and error wrapping
result = job.execute({"required_key": "value", "other": "data"})

if result.success:
    print(result.output)  # ['required_key: value', 'other: data']
else:
    print(result.error)   # Validation or processing error message
```