use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::config::ConfigOptions;
use datafusion::common::Result;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_expr::{PhysicalExpr, expressions::Column};
use std::sync::Arc;

#[derive(Debug)]
pub struct PartialAggregationOptimizer;

impl PhysicalOptimizerRule for PartialAggregationOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.optimize_plan(plan)
    }

    fn name(&self) -> &str {
        "partial_aggregation_optimizer"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

impl PartialAggregationOptimizer {
    fn optimize_plan(&self, plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        println!("[DEBUG] === Optimizing plan node: {} ===", plan.name());
        
        // Recursively optimize children first
        let optimized_children: Result<Vec<_>> = plan.children()
            .into_iter()
            .map(|child| self.optimize_plan(Arc::clone(child)))
            .collect();
        let optimized_children = optimized_children?;

        // Handle AggregateExec: convert to Partial mode
        if let Some(agg) = plan.as_any().downcast_ref::<AggregateExec>() {
            println!("[DEBUG] Found AggregateExec, mode: {:?}", agg.mode());
            println!("[DEBUG] Aggregate output schema: {:?}", agg.schema().fields().iter().map(|f| f.name()).collect::<Vec<_>>());
            if !matches!(agg.mode(), &AggregateMode::Partial) {
                let new_agg = AggregateExec::try_new(
                    AggregateMode::Partial,
                    agg.group_expr().clone(),
                    agg.aggr_expr().to_vec(),
                    agg.filter_expr().to_vec(),
                    optimized_children[0].clone(),
                    optimized_children[0].schema(),
                )?;
                println!("[DEBUG] Created new Partial aggregate, output schema: {:?}", new_agg.schema().fields().iter().map(|f| f.name()).collect::<Vec<_>>());
                return Ok(Arc::new(new_agg));
            }
            return plan.with_new_children(optimized_children);
        }

        // Handle ProjectionExec: fix column references to match modified children
        if let Some(proj) = plan.as_any().downcast_ref::<ProjectionExec>() {
            println!("[DEBUG] === HANDLING ProjectionExec ===");
            let new_input = optimized_children[0].clone();
            let input_schema = new_input.schema();
            println!("[DEBUG] ProjectionExec - Input schema fields: {:?}", input_schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>());
            println!("[DEBUG] ProjectionExec - Current projection exprs: {:?}", proj.expr().iter().map(|(_, name)| name).collect::<Vec<_>>());
            
            let new_exprs: Vec<_> = proj.expr()
                .iter()
                .enumerate()
                .map(|(idx, (expr, name))| {
                    println!("[DEBUG] Processing projection[{}]: name={}, expr={:?}", idx, name, expr);
                    if let Some(col) = expr.as_any().downcast_ref::<Column>() {
                        let col_name = col.name();
                        println!("[DEBUG]   Column name: {}, index: {}", col_name, col.index());
                        
                        // Try to find by column name first
                        if let Ok(field) = input_schema.field_with_name(col_name) {
                            let field_name = field.name().clone();
                            println!("[DEBUG]   Found field by name '{}' -> '{}'", col_name, field_name);
                            let new_col = Column::new(field_name.as_str(), col.index());
                            return (Arc::new(new_col) as Arc<dyn PhysicalExpr>, name.clone());
                        }
                        
                        // Try to find by index if name lookup fails (handles aggregate mode changes)
                        if col.index() < input_schema.fields().len() {
                            let field = input_schema.field(col.index());
                            let field_name = field.name().clone();
                            println!("[DEBUG]   Found field by index {} -> '{}', keeping output name '{}'", col.index(), field_name, name);
                            let new_col = Column::new(field_name.as_str(), col.index());
                            // Keep the original output name from the projection
                            return (Arc::new(new_col) as Arc<dyn PhysicalExpr>, name.clone());
                        }
                        
                        println!("[DEBUG]   Field '{}' not found in input schema", col_name);
                    }
                    println!("[DEBUG]   Using original expr and name: {}", name);
                    (Arc::clone(expr) as Arc<dyn PhysicalExpr>, name.clone())
                })
                .collect();
            
            println!("[DEBUG] ProjectionExec - New projection exprs: {:?}", new_exprs.iter().map(|(_, name)| name).collect::<Vec<_>>());
            let new_proj = Arc::new(ProjectionExec::try_new(new_exprs, new_input)?);
            println!("[DEBUG] ProjectionExec - Created new projection with schema: {:?}", new_proj.schema().fields().iter().map(|f| f.name()).collect::<Vec<_>>());
            return Ok(new_proj);
        }

        // For all other nodes, just update with optimized children
        println!("[DEBUG] Returning plan with new children: {}", plan.name());
        plan.with_new_children(optimized_children)
    }
}
