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
        false
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

        // Handle ProjectionExec: pass through all aggregate state columns
        if let Some(_proj) = plan.as_any().downcast_ref::<ProjectionExec>() {
            let new_input = optimized_children[0].clone();
            let input_schema = new_input.schema();
            
            // Pass through all fields from the partial aggregate
            let new_exprs: Vec<_> = input_schema.fields()
                .iter()
                .enumerate()
                .map(|(idx, field)| {
                    let col = Column::new(field.name(), idx);
                    (Arc::new(col) as Arc<dyn PhysicalExpr>, field.name().clone())
                })
                .collect();

            return Ok(Arc::new(ProjectionExec::try_new(new_exprs, new_input)?));
        }

        // For all other nodes, just update with optimized children
        println!("[DEBUG] Returning plan with new children: {}", plan.name());
        plan.with_new_children(optimized_children)
    }
}
