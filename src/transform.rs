//! Named transform registry and edge transform evaluation.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::RwLock;

use loom_domain::EdgeTransform;
use plexus_base::Value;

use crate::error::EngineError;

/// A pinned, heap-allocated future that resolves to `T` and is `Send`.
pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// A registry of named async transform functions keyed by name.
///
/// Both sync and async callables can be registered:
/// - [`register`](TransformRegistry::register) — wraps a sync `Fn` in an async block.
/// - [`register_async`](TransformRegistry::register_async) — stores a native async fn directly.
///
/// A shared [`rhai::Engine`] is embedded for evaluating [`EdgeTransform::Script`]
/// variants. The engine has conservative safety limits applied by default
/// (`max_operations = 50_000`, `max_call_levels = 32`).
#[derive(Clone)]
pub struct TransformRegistry {
    fns: HashMap<String, Arc<dyn Fn(Vec<Value>) -> BoxFuture<Value> + Send + Sync>>,
    pub(crate) script_engine: Arc<rhai::Engine>,
    ast_cache: Arc<RwLock<HashMap<String, rhai::AST>>>,
}

impl Default for TransformRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl TransformRegistry {
    /// Create an empty registry with a sandboxed Rhai scripting engine.
    ///
    /// The engine has the following safety limits:
    /// - Maximum 50,000 operations per script evaluation.
    /// - Maximum 32 nested function call levels.
    pub fn new() -> Self {
        let mut engine = rhai::Engine::new();
        engine.set_max_operations(50_000u64);
        engine.set_max_call_levels(32);
        TransformRegistry {
            fns: HashMap::new(),
            script_engine: Arc::new(engine),
            ast_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a **synchronous** transform function.
    ///
    /// The function is wrapped in an async block so it integrates seamlessly
    /// with the rest of the async execution path.
    pub fn register(
        &mut self,
        name: impl Into<String>,
        f: impl Fn(Vec<Value>) -> Value + Send + Sync + 'static,
    ) {
        let f = Arc::new(f);
        self.fns.insert(
            name.into(),
            Arc::new(move |inputs: Vec<Value>| {
                let f = Arc::clone(&f);
                Box::pin(async move { f(inputs) }) as BoxFuture<Value>
            }),
        );
    }

    /// Register a **native async** transform function.
    ///
    /// Use this when your transform needs to `await` (e.g. database lookups,
    /// HTTP calls, inter-task channels).
    pub fn register_async<F, Fut>(
        &mut self,
        name: impl Into<String>,
        f: F,
    )
    where
        F: Fn(Vec<Value>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Value> + Send + 'static,
    {
        self.fns.insert(
            name.into(),
            Arc::new(move |inputs: Vec<Value>| {
                Box::pin(f(inputs)) as BoxFuture<Value>
            }),
        );
    }

    /// Retrieve a named transform function by name.
    pub fn get(
        &self,
        name: &str,
    ) -> Option<&Arc<dyn Fn(Vec<Value>) -> BoxFuture<Value> + Send + Sync>> {
        self.fns.get(name)
    }

    fn get_or_compile_ast(&self, code: &str) -> Result<rhai::AST, EngineError> {
        {
            let cache = self
                .ast_cache
                .read()
                .map_err(|_| EngineError::ExecutionFailed("script AST cache read lock poisoned".to_string()))?;
            if let Some(ast) = cache.get(code).cloned() {
                return Ok(ast);
            }
        }

        let mut cache = self
            .ast_cache
            .write()
            .map_err(|_| EngineError::ExecutionFailed("script AST cache write lock poisoned".to_string()))?;

        if let Some(ast) = cache.get(code).cloned() {
            return Ok(ast);
        }

        let compiled = self
            .script_engine
            .compile(code)
            .map_err(|e| EngineError::ExecutionFailed(format!("script compile failed: {e}")))?;
        cache.insert(code.to_string(), compiled.clone());
        Ok(compiled)
    }
}

/// Evaluate an [`EdgeTransform`] against a list of input values.
///
/// - `PassThrough` — returns the first input value, or `Value::Null` if empty.
/// - `Named(name)` — looks up `name` in `registry`, calls it, and awaits the result.
/// - `Inline(f)` — calls `f` synchronously (sync closure, no await).
/// - `Script(code)` — evaluates `code` via the embedded Rhai engine. The script
///   receives the inputs as an `inputs` array variable and must return a value.
pub async fn eval_transform(
    transform: &EdgeTransform,
    inputs: Vec<Value>,
    registry: &TransformRegistry,
) -> Result<Value, EngineError> {
    match transform {
        EdgeTransform::PassThrough => Ok(inputs.into_iter().next().unwrap_or(Value::Null)),
        EdgeTransform::Named(name) => {
            let f = registry
                .get(name)
                .ok_or_else(|| EngineError::TransformNotFound(name.clone()))?;
            Ok(f(inputs).await)
        }
        EdgeTransform::Inline(f) => Ok(f(&inputs)),
        EdgeTransform::Script(code) => {
            let inputs_arr: rhai::Array =
                inputs.into_iter().map(value_to_dynamic).collect();
            let mut scope = rhai::Scope::new();
            scope.push("inputs", inputs_arr);

            let ast = registry.get_or_compile_ast(code)?;

            let dyn_result = registry
                .script_engine
                .eval_ast_with_scope::<rhai::Dynamic>(&mut scope, &ast)
                .map_err(|e| EngineError::ExecutionFailed(format!("script eval failed: {e}")))?;
            Ok(dynamic_to_value(dyn_result))
        }
    }
}

// ---------------------------------------------------------------------------
// Rhai ↔ Value conversions
// ---------------------------------------------------------------------------

/// Convert a [`Value`] into a [`rhai::Dynamic`] for use inside Rhai scripts.
fn value_to_dynamic(v: Value) -> rhai::Dynamic {
    match v {
        Value::Null => rhai::Dynamic::UNIT,
        Value::Bool(b) => rhai::Dynamic::from(b),
        Value::Int(i) => rhai::Dynamic::from(i),
        Value::Float(f) => rhai::Dynamic::from(f),
        Value::Str(s) => rhai::Dynamic::from(s),
        Value::Bytes(b) => {
            let blob: rhai::Blob = b;
            rhai::Dynamic::from_blob(blob)
        }
        Value::List(l) => {
            let arr: rhai::Array = l.into_iter().map(value_to_dynamic).collect();
            rhai::Dynamic::from_array(arr)
        }
        Value::Map(m) => {
            let map: rhai::Map = m
                .into_iter()
                .map(|(k, v)| (k.into(), value_to_dynamic(v)))
                .collect();
            rhai::Dynamic::from_map(map)
        }
    }
}

/// Convert a [`rhai::Dynamic`] back to a [`Value`].
///
/// Unknown Rhai types (custom objects, etc.) map to [`Value::Null`].
fn dynamic_to_value(d: rhai::Dynamic) -> Value {
    if d.is_unit() {
        Value::Null
    } else if d.is::<bool>() {
        Value::Bool(d.cast::<bool>())
    } else if d.is::<i64>() {
        Value::Int(d.cast::<i64>())
    } else if d.is::<f64>() {
        Value::Float(d.cast::<f64>())
    } else if d.is::<rhai::ImmutableString>() {
        Value::Str(d.cast::<rhai::ImmutableString>().to_string())
    } else if d.is::<rhai::Blob>() {
        Value::Bytes(d.cast::<rhai::Blob>())
    } else if d.is::<rhai::Array>() {
        Value::List(
            d.cast::<rhai::Array>()
                .into_iter()
                .map(dynamic_to_value)
                .collect(),
        )
    } else if d.is::<rhai::Map>() {
        let map: BTreeMap<String, Value> = d
            .cast::<rhai::Map>()
            .into_iter()
            .map(|(k, v)| (k.to_string(), dynamic_to_value(v)))
            .collect();
        Value::Map(map)
    } else {
        Value::Null
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use plexus_base::Value;

    #[tokio::test]
    async fn passthrough_returns_first() {
        let reg = TransformRegistry::new();
        let result = eval_transform(
            &EdgeTransform::PassThrough,
            vec![Value::Int(42), Value::Int(99)],
            &reg,
        )
        .await
        .unwrap();
        assert_eq!(result, Value::Int(42));
    }

    #[tokio::test]
    async fn passthrough_empty_returns_null() {
        let reg = TransformRegistry::new();
        let result = eval_transform(&EdgeTransform::PassThrough, vec![], &reg)
            .await
            .unwrap();
        assert_eq!(result, Value::Null);
    }

    #[tokio::test]
    async fn named_sync_transform_found() {
        let mut reg = TransformRegistry::new();
        reg.register("double", |inputs| {
            if let Some(Value::Int(n)) = inputs.first() {
                Value::Int(n * 2)
            } else {
                Value::Null
            }
        });
        let result = eval_transform(
            &EdgeTransform::Named("double".to_string()),
            vec![Value::Int(5)],
            &reg,
        )
        .await
        .unwrap();
        assert_eq!(result, Value::Int(10));
    }

    #[tokio::test]
    async fn named_async_transform_found() {
        let mut reg = TransformRegistry::new();
        reg.register_async("async_double", |inputs| async move {
            if let Some(Value::Int(n)) = inputs.first() {
                Value::Int(n * 2)
            } else {
                Value::Null
            }
        });
        let result = eval_transform(
            &EdgeTransform::Named("async_double".to_string()),
            vec![Value::Int(6)],
            &reg,
        )
        .await
        .unwrap();
        assert_eq!(result, Value::Int(12));
    }

    #[tokio::test]
    async fn named_transform_not_found() {
        let reg = TransformRegistry::new();
        let err = eval_transform(
            &EdgeTransform::Named("missing".to_string()),
            vec![],
            &reg,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, EngineError::TransformNotFound(_)));
    }

    #[tokio::test]
    async fn inline_transform_called() {
        let reg = TransformRegistry::new();
        let t = EdgeTransform::Inline(Arc::new(|_inputs| Value::Bool(true)));
        let result = eval_transform(&t, vec![], &reg).await.unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[tokio::test]
    async fn script_transform_arithmetic() {
        let reg = TransformRegistry::new();
        let t = EdgeTransform::Script("inputs[0] + inputs[1]".to_string());
        let result = eval_transform(&t, vec![Value::Int(3), Value::Int(4)], &reg)
            .await
            .unwrap();
        assert_eq!(result, Value::Int(7));
    }

    #[tokio::test]
    async fn script_transform_string_passthrough() {
        let reg = TransformRegistry::new();
        let t = EdgeTransform::Script("inputs[0]".to_string());
        let result = eval_transform(&t, vec![Value::Str("hello".to_string())], &reg)
            .await
            .unwrap();
        assert_eq!(result, Value::Str("hello".to_string()));
    }

    #[tokio::test]
    async fn script_transform_reuses_cached_ast() {
        let reg = TransformRegistry::new();
        let t = EdgeTransform::Script("inputs[0] + 1".to_string());

        let first = eval_transform(&t, vec![Value::Int(1)], &reg).await.unwrap();
        let second = eval_transform(&t, vec![Value::Int(2)], &reg).await.unwrap();

        assert_eq!(first, Value::Int(2));
        assert_eq!(second, Value::Int(3));
        assert_eq!(reg.ast_cache.read().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn script_transform_concurrent_uses_single_cached_ast() {
        let reg = TransformRegistry::new();
        let t = EdgeTransform::Script("inputs[0] * 2".to_string());

        let mut tasks = Vec::new();
        for i in 0_i64..16_i64 {
            let reg_clone = reg.clone();
            let t_clone = t.clone();
            tasks.push(tokio::spawn(async move {
                eval_transform(&t_clone, vec![Value::Int(i)], &reg_clone).await
            }));
        }

        for (i, task) in tasks.into_iter().enumerate() {
            let value = task.await.unwrap().unwrap();
            assert_eq!(value, Value::Int((i as i64) * 2));
        }

        assert_eq!(reg.ast_cache.read().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn script_transform_error_on_invalid_code() {
        let reg = TransformRegistry::new();
        let t = EdgeTransform::Script("!!!invalid!!!".to_string());
        let err = eval_transform(&t, vec![], &reg).await.unwrap_err();
        assert!(matches!(err, EngineError::ExecutionFailed(_)));
        assert_eq!(reg.ast_cache.read().unwrap().len(), 0);
    }
}
