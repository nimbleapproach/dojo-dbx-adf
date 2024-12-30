# 🗄️ Master Data Management Framework

## Summary
The MDM POC demonstrates dimensional modeling standards and source data mastering capabilities.

1. [📥 Stage Data Process](#staging-process)
   - Data Standardization & Mapping
   - Advanced Matching (Exact/Fuzzy)
   - Data Quality Management
   - Priority-based Record Mastering
   - Source System Hierarchy Configuration

2. [⚙️ Modeling Framework](#modelling-framework-demo)
   - Entity Configuration 
   - Target Schema Generation
   - Automated Processing Pipeline
   - SCD Type 2 Support

## Core Features
- 🎯 Priority-based Entity Mastering
- 🔍 Configurable Match Thresholds
- 📊 Quality Monitoring
- 🔄 Version Control (SCD Type 2)
- 🏗️ Metadata-driven Architecture

## 📥 Stage Data {#staging-process}

### Key Functions
**map_parent_child_keys**
- Prioritises records using configurable hierarchy
- Handles multiple source systems
- Maintains data lineage

```python
def map_parent_child_keys(
    df,
    key_cols=["ManufacturerItemNo", "VendorName", "SourceSystem"],
    priority_map = {
        "System_A": 1,  # Primary
        "System_B": 2,  # Secondary
        "System_C": 3   # Tertiary
    }
)
```

### Data Flow
:::mermaid
graph TD
    A[Source Data] --> B[Priority Engine]
    B --> C[Stage Tables]
    B --> D[Quality Tables]
    C --> E[Meta Framework]
    E --> F[Target Tables]
:::

### Processing Pipeline
1. **Priority-based Mastering**
   - Entity-specific staging (e.g., `dim_vendor_stg`)
   - Master reference tables (e.g., `dim_master_vendor_stg`) 
   - Hierarchy-based golden record selection

2. **Meta Framework Processing**
   - Schema-driven transformations
   - SCD Type 2 versioning
   - Automated target loading

:::mermaid
graph TD
    A[Staging Layer] --> B[Processing Layer]
    B --> C[Target Layer]
:::

### 📑 Dependencies
- `nb-orion-common`: Core utilities
- `meta.json`: Configuration metadata
- `nb-orion-process-fact-model.py`: Pipeline orchestration

The framework supports extensible entity modeling while maintaining data quality and versioning standards.