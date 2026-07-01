import React, { useState, useEffect, useRef, useMemo } from 'react';

// ==========================================
// MẪU DỮ LIỆU ĐẶC TẢ KHỞI TẠO (9.0.0 SPEC)
// ==========================================
const INITIAL_WORKFLOW_SPEC = {
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "workflow_id": "wf_ems_spare_parts_001",
  "name": "EmsSparePartsEnterpriseWorkflow",
  "version": "9.0.0",
  "tenant_id": "tenant_lgd_factory_01",
  "metadata": {
    "author": "Enterprise Architecture Team",
    "description": "Hệ thống tự động hóa và tích hợp báo cáo kho vật tư dự phòng EMS phân tán",
    "created_at": "2026-06-30T20:00:00Z",
    "last_modified_by": "System Architect"
  },
  "default_error_policy": {
    "retry": {
      "max_attempts": 2,
      "backoff": {
        "type": "FIXED",
        "delay_ms": 2000
      }
    },
    "on_failure": "FAIL_FAST"
  },
  "schema_registry": {
    "Schema_InputItems": {
      "type": "OBJECT",
      "properties": {
        "items": { "type": "ARRAY", "items": { "type": "STRING" }, "min_items": 1 },
        "target_subinventory": { "type": "STRING", "enum": ["SP-AA-UNL", "SP-C-NEW", "ALL"] }
      },
      "required": ["items", "target_subinventory"]
    },
    "Schema_RawEmsRows": {
      "type": "DATASET",
      "columns": [
        { "name": "lpnId", "type": "STRING", "nullable": false },
        { "name": "item", "type": "STRING", "nullable": false },
        { "name": "subinventoryCode", "type": "STRING", "nullable": false },
        { "name": "onhandQuantity", "type": "DECIMAL", "precision": 18, "scale": 4, "nullable": false },
        { "name": "availableQuantity", "type": "DECIMAL", "precision": 18, "scale": 4, "nullable": false },
        { "name": "poWaitingQty", "type": "DECIMAL", "precision": 18, "scale": 4, "default": 0.0 },
        { "name": "notReceivingQty", "type": "DECIMAL", "precision": 18, "scale": 4, "default": 0.0 },
        { "name": "stopPo", "type": "STRING", "maxLength": 1, "default": "N" },
        { "name": "vendorName", "type": "STRING", "nullable": true },
        { "name": "safetyStockQty", "type": "DECIMAL", "precision": 18, "scale": 4, "default": 0.0 }
      ]
    }
  },
  "context": {
    "scopes": {
      "global": {
        "variables": {
          "default_user_id": "658359",
          "vat_rate": 0.10
        },
        "secrets": {
          "ems_api_key": "vault://lgd-secrets/ems/api-key"
        }
      },
      "env_mappings": {
        "PROD": {
          "ems_host": "http://ems.lgdisplay.com:9000",
          "max_connections": 50
        },
        "UAT": {
          "ems_host": "http://uat-ems.lgdisplay.com:9000",
          "max_connections": 10
        }
      }
    }
  },
  "nodes": {
    "node_user_input": {
      "type": "USER_INTERACTION",
      "label": "Giao diện nhập liệu thông tin lô hàng",
      "depends_on": [],
      "config": {
        "output_schema": "Schema_InputItems",
        "fields": [
          {
            "name": "items",
            "type": "TEXT_AREA",
            "validation": {
              "regex": "^[A-Za-z0-9,\\s\\n]+$",
              "error_message": "Danh sách vật tư không hợp lệ."
            }
          },
          {
            "name": "target_subinventory",
            "type": "SELECT",
            "default_value": "ALL"
          }
        ]
      }
    },
    "node_fetch_ems_data": {
      "type": "CONNECTOR_HTTP",
      "label": "Bắn API truy vấn hệ thống EMS",
      "depends_on": ["node_user_input"],
      "config": {
        "connection": {
          "url": "${{context.env_mappings.PROD.ems_host}}/emsspareparts/sp101/sp110Nav/RetStockStatusList.lgdn",
          "method": "POST",
          "timeout_ms": 5000,
          "headers": {
            "Content-Type": "application/json",
            "X-API-Key": "${{context.global.secrets.ems_api_key}}"
          }
        },
        "execution": {
          "loop_over": "$.steps.node_user_input.output.items",
          "strategy": "CONCURRENT",
          "max_concurrency": 10,
          "aggregation_strategy": "UNION_ALL",
          "cache": {
            "enabled": true,
            "ttl_seconds": 600,
            "key_pattern": "ems:${{tenant_id}}:stock_status:${{loop_item}}"
          }
        },
        "request_builder": {
          "Parameters": {
            "_ems_userId": "${{context.global.variables.default_user_id}}",
            "_ems_organizationId": "28753"
          },
          "Datasets": {
            "ds_search": {
              "ColumnInfo": ["item", "organizationId"],
              "Rows": [
                {
                  "item": "${{loop_item}}",
                  "organizationId": "28753"
                }
              ]
            }
          }
        },
        "output_schema": "Schema_RawEmsRows"
      },
      "error_policy": {
        "retry": {
          "max_attempts": 3,
          "backoff": {
            "type": "EXPONENTIAL",
            "delay_ms": 1000,
            "max_delay_ms": 10000,
            "jitter": true
          }
        },
        "circuit_breaker": {
          "failure_threshold_percentage": 50,
          "recovery_timeout_ms": 30000
        },
        "on_failure": "FAIL_FAST"
      }
    },
    "node_data_pipeline": {
      "type": "DATA_PIPELINE",
      "label": "Động cơ xử lý dữ liệu nâng cao",
      "depends_on": ["node_fetch_ems_data"],
      "config": {
        "pipeline": [
          {
            "id": "op_01",
            "operator": "UNNEST",
            "inputs": {
              "input_object": "$.steps.node_user_input.output"
            },
            "config": {
              "array_path": "$.items",
              "alias_column": "target_item"
            },
            "output_key": "ds_items_unnested"
          },
          {
            "id": "op_02",
            "operator": "JOIN",
            "inputs": {
              "left_dataset": "$.pipeline_variables.ds_items_unnested",
              "right_dataset": "$.steps.node_fetch_ems_data.output"
            },
            "config": {
              "join_type": "INNER",
              "on": {
                "left_key": "target_item",
                "right_key": "item"
              }
            },
            "output_key": "ds_joined"
          },
          {
            "id": "op_03",
            "operator": "FILTER",
            "inputs": {
              "input_dataset": "$.pipeline_variables.ds_joined"
            },
            "config": {
              "condition": "subinventoryCode IN if($.steps.node_user_input.output.target_subinventory == 'ALL', ['SP-AA-UNL', 'SP-C-NEW'], [$.steps.node_user_input.output.target_subinventory])"
            },
            "output_key": "ds_filtered"
          },
          {
            "id": "op_04",
            "operator": "TRANSFORM",
            "inputs": {
              "input_dataset": "$.pipeline_variables.ds_filtered"
            },
            "config": {
              "operations": {
                "lpnId": "$.lpnId",
                "partNo": "$.item",
                "subinventory": "$.subinventoryCode",
                "onhandQty": "to_decimal($.onhandQuantity)",
                "availableQty": "to_decimal($.availableQuantity)",
                "incomingQty": "to_decimal($.poWaitingQty) + to_decimal($.notReceivingQty)",
                "poBlockStatus": "if($.stopPo == 'Y', 'Blocked', 'Allowed')",
                "needToOrder": "if(to_decimal($.availableQuantity) < to_decimal($.safetyStockQty), 'Yes', 'No')"
              }
            },
            "output_key": "ds_transformed"
          },
          {
            "id": "op_05",
            "operator": "AGGREGATE",
            "inputs": {
              "input_dataset": "$.pipeline_variables.ds_transformed"
            },
            "config": {
              "group_by": ["partNo", "subinventory", "needToOrder"],
              "aggregations": [
                { "column": "availableQty", "function": "SUM", "alias": "sum_available" },
                { "column": "incomingQty", "function": "SUM", "alias": "sum_incoming" }
              ]
            },
            "output_key": "ds_aggregated"
          },
          {
            "id": "op_06",
            "operator": "PIVOT",
            "inputs": {
              "input_dataset": "$.pipeline_variables.ds_aggregated"
            },
            "config": {
              "index": ["subinventory"],
              "pivot_column": "partNo",
              "value_column": "sum_available"
            },
            "output_key": "ds_pivoted_report"
          },
          {
            "id": "op_07",
            "operator": "ACCUMULATE",
            "inputs": {
              "input_dataset": "$.pipeline_variables.ds_pivoted_report"
            },
            "config": {
              "accumulator_key": "global_cache:${{tenant_id}}:${{workflow_id}}:last_processed_report",
              "merge_strategy": "UPSERT"
            }
          }
        ]
      }
    },
    "node_display_report": {
      "type": "USER_DISPLAY",
      "label": "Kết xuất dữ liệu lên lưới báo cáo",
      "depends_on": ["node_data_pipeline"],
      "config": {
        "input_dataset": "$.steps.node_data_pipeline.pipeline_output.ds_pivoted_report",
        "ui_component": "DATA_GRID",
        "settings": {
          "allow_excel_export": true,
          "enable_filtering": true,
          "theme": "DARK_MODE"
        }
      }
    }
  }
};

// Toạ độ mặc định tuyệt đẹp cho các node để render ban đầu trên canvas
const INITIAL_NODE_POSITIONS = {
  "node_user_input": { x: 80, y: 150 },
  "node_fetch_ems_data": { x: 380, y: 150 },
  "node_data_pipeline": { x: 680, y: 150 },
  "node_display_report": { x: 980, y: 150 }
};

export default function App() {
  // Trạng thái cấu trúc Workflow chính
  const [workflow, setWorkflow] = useState(INITIAL_WORKFLOW_SPEC);
  const [selectedNodeId, setSelectedNodeId] = useState("node_data_pipeline");
  const [activeTab, setActiveTab] = useState("canvas"); // "canvas" | "pipeline" | "schemas" | "context"
  const [selectedPipelineOpIndex, setSelectedPipelineOpIndex] = useState(0);

  // Trạng thái Canvas Drag & Drop
  const [nodePositions, setNodePositions] = useState(INITIAL_NODE_POSITIONS);
  const [draggingNodeId, setDraggingNodeId] = useState(null);
  const [dragOffset, setDragOffset] = useState({ x: 0, y: 0 });
  const [canvasScale, setCanvasScale] = useState(1.0);
  const canvasRef = useRef(null);

  // Trạng thái UI phụ trợ
  const [toast, setToast] = useState(null);
  const [jsonMinified, setJsonMinified] = useState(false);
  const [activeSchemaTab, setActiveSchemaTab] = useState("Schema_RawEmsRows");
  const [activeExprTab, setActiveExprTab] = useState("basic"); // "basic" | "advanced"
  
  // Trạng thái Expression Builder tạm thời
  const [exprSelectedColumn, setExprSelectedColumn] = useState("subinventoryCode");
  const [exprOperator, setExprOperator] = useState("IN");
  const [exprValue, setExprValue] = useState("['SP-AA-UNL', 'SP-C-NEW']");

  // Undo/Redo History Stack
  const [history, setHistory] = useState([]);
  const [historyPointer, setHistoryPointer] = useState(-1);

  // Lưu lịch sử mỗi lần cập nhật workflow
  const pushHistory = (newWorkflow) => {
    const updatedHistory = history.slice(0, historyPointer + 1);
    updatedHistory.push(JSON.parse(JSON.stringify(newWorkflow)));
    setHistory(updatedHistory);
    setHistoryPointer(updatedHistory.length - 1);
  };

  const handleSetWorkflow = (updater) => {
    let nextWorkflow;
    if (typeof updater === 'function') {
      nextWorkflow = updater(workflow);
    } else {
      nextWorkflow = updater;
    }
    setWorkflow(nextWorkflow);
    pushHistory(nextWorkflow);
  };

  const handleUndo = () => {
    if (historyPointer > 0) {
      const ptr = historyPointer - 1;
      setHistoryPointer(ptr);
      setWorkflow(JSON.parse(JSON.stringify(history[ptr])));
      showToast("Đã Undo thao tác trước");
    }
  };

  const handleRedo = () => {
    if (historyPointer < history.length - 1) {
      const ptr = historyPointer + 1;
      setHistoryPointer(ptr);
      setWorkflow(JSON.parse(JSON.stringify(history[ptr])));
      showToast("Đã Redo thao tác");
    }
  };

  // Khởi tạo lịch sử lúc ban đầu
  useEffect(() => {
    if (history.length === 0) {
      setHistory([JSON.parse(JSON.stringify(INITIAL_WORKFLOW_SPEC))]);
      setHistoryPointer(0);
    }
  }, []);

  const showToast = (message, type = "success") => {
    setToast({ message, type });
    setTimeout(() => setToast(null), 3500);
  };

  // ==========================================
  // REAL-TIME VALIDATION ENGINE
  // ==========================================
  const validationErrors = useMemo(() => {
    const errors = [];
    const nodes = workflow.nodes || {};

    // 1. Kiểm tra chu trình (Cycle Detection)
    const checkCycles = () => {
      const adj = {};
      const visited = {};
      const recStack = {};

      for (const id of Object.keys(nodes)) {
        adj[id] = nodes[id].depends_on || [];
        visited[id] = false;
        recStack[id] = false;
      }

      const isCyclicUtil = (curr) => {
        if (!visited[curr]) {
          visited[curr] = true;
          recStack[curr] = true;

          const deps = adj[curr] || [];
          for (const dep of deps) {
            if (!nodes[dep]) continue; // Lỗi thiếu node sẽ được bắt ở phần sau
            if (!visited[dep] && isCyclicUtil(dep)) {
              return true;
            } else if (recStack[dep]) {
              return true;
            }
          }
        }
        recStack[curr] = false;
        return false;
      };

      for (const id of Object.keys(nodes)) {
        if (isCyclicUtil(id)) return true;
      }
      return false;
    };

    if (checkCycles()) {
      errors.push({
        severity: "CRITICAL",
        category: "DAG Engine",
        message: "Phát hiện chu trình vòng lặp phụ thuộc vô hạn (Circular Dependency)! Luồng thực thi bị chặn hoàn toàn.",
        code: "CYCLE_DETECTION_ERROR"
      });
    }

    // 2. Kiểm tra node thiếu hoặc mồ côi
    Object.entries(nodes).forEach(([id, node]) => {
      const deps = node.depends_on || [];
      deps.forEach(dep => {
        if (!nodes[dep]) {
          errors.push({
            severity: "HIGH",
            category: "DAG Scheduler",
            nodeId: id,
            message: `Node [${id}] phụ thuộc vào node không tồn tại: [${dep}].`,
            code: "MISSING_DEPENDENCY"
          });
        }
      });

      // 3. Kiểm tra Output Schema và Schema Registry
      if (node.type === "USER_INTERACTION" || node.type === "CONNECTOR_HTTP") {
        const outSchema = node.config?.output_schema;
        if (outSchema && !workflow.schema_registry?.[outSchema]) {
          errors.push({
            severity: "MEDIUM",
            category: "Schema Registry",
            nodeId: id,
            message: `Schema đầu ra [${outSchema}] của node [${id}] chưa được định nghĩa trong Registry.`,
            code: "SCHEMA_NOT_FOUND"
          });
        }
      }

      // 4. Kiểm tra cấu hình kết nối của Connector HTTP
      if (node.type === "CONNECTOR_HTTP") {
        if (!node.config?.connection?.url) {
          errors.push({
            severity: "HIGH",
            category: "Connector HTTP",
            nodeId: id,
            message: `Node Connector [${id}] thiếu tham số đường dẫn Endpoint URL.`,
            code: "MISSING_HTTP_URL"
          });
        }
      }

      // 5. Kiểm tra tính hợp lệ của Data Pipeline Operators
      if (node.type === "DATA_PIPELINE") {
        const pipeline = node.config?.pipeline || [];
        if (pipeline.length === 0) {
          errors.push({
            severity: "MEDIUM",
            category: "Data Pipeline",
            nodeId: id,
            message: `Pipeline [${id}] không chứa toán tử xử lý nào.`,
            code: "EMPTY_PIPELINE"
          });
        } else {
          pipeline.forEach((op, index) => {
            if (!op.operator) {
              errors.push({
                severity: "HIGH",
                category: "Data Pipeline",
                nodeId: id,
                message: `Pipeline [${id}] - Toán tử thứ #${index + 1} chưa cấu hình loại Operator.`,
                code: "MISSING_OPERATOR_TYPE"
              });
            }
            if (!op.output_key && op.operator !== "ACCUMULATE") {
              errors.push({
                severity: "LOW",
                category: "Data Pipeline",
                nodeId: id,
                message: `Toán tử [${op.operator}] tại bước #${index + 1} thiếu thuộc tính [output_key].`,
                code: "MISSING_OUTPUT_KEY"
              });
            }
          });
        }
      }
    });

    return errors;
  }, [workflow]);

  // Kiểm tra xem node có bị lỗi hay không
  const getNodeSeverity = (nodeId) => {
    const nodeErrors = validationErrors.filter(e => e.nodeId === nodeId);
    if (nodeErrors.some(e => e.severity === "CRITICAL" || e.severity === "HIGH")) return "border-rose-500 shadow-rose-950/20";
    if (nodeErrors.some(e => e.severity === "MEDIUM")) return "border-amber-500 shadow-amber-950/20";
    return "border-slate-700 hover:border-violet-500 hover:shadow-violet-950/20";
  };

  // ==========================================
  // HANDLERS CHO DRAG & DROP CANVAS
  // ==========================================
  const handleCanvasMouseDown = (e) => {
    if (e.target === canvasRef.current || e.target.id === "svg-overlay") {
      setDraggingNodeId(null);
    }
  };

  const handleNodeDragStart = (e, id) => {
    e.stopPropagation();
    setDraggingNodeId(id);
    const rect = e.currentTarget.getBoundingClientRect();
    setDragOffset({
      x: e.clientX - rect.left,
      y: e.clientY - rect.top
    });
    setSelectedNodeId(id);
  };

  const handleCanvasMouseMove = (e) => {
    if (!draggingNodeId) return;
    const canvasBounds = canvasRef.current.getBoundingClientRect();
    // Tính toán toạ độ tương đối dựa trên tỉ lệ zoom
    const x = (e.clientX - canvasBounds.left - dragOffset.x) / canvasScale;
    const y = (e.clientY - canvasBounds.top - dragOffset.y) / canvasScale;

    setNodePositions(prev => ({
      ...prev,
      [draggingNodeId]: { x: Math.max(10, Math.round(x)), y: Math.max(10, Math.round(y)) }
    }));
  };

  const handleCanvasMouseUp = () => {
    setDraggingNodeId(null);
  };

  // Tạo một node mới trực quan
  const handleAddNode = (type) => {
    const newId = `node_${type.toLowerCase()}_${Date.now().toString().slice(-4)}`;
    const labels = {
      "USER_INTERACTION": "Giao diện đầu vào mới",
      "CONNECTOR_HTTP": "Truy vấn API HTTP mới",
      "DATA_PIPELINE": "Xử lý Pipeline mới",
      "USER_DISPLAY": "Giao diện hiển thị mới"
    };

    const defaultConfigs = {
      "USER_INTERACTION": { output_schema: "Schema_InputItems", fields: [] },
      "CONNECTOR_HTTP": {
        connection: { url: "http://api.endpoint/v1/data", method: "POST", timeout_ms: 3000 },
        output_schema: "Schema_RawEmsRows"
      },
      "DATA_PIPELINE": { pipeline: [] },
      "USER_DISPLAY": { input_dataset: "$.steps.node_data_pipeline.pipeline_output.ds_pivoted_report", ui_component: "DATA_GRID", settings: {} }
    };

    // Đặt vị trí node mới ngay trung tâm canvas
    const x = 300 + Math.random() * 100;
    const y = 200 + Math.random() * 100;

    setNodePositions(prev => ({ ...prev, [newId]: { x, y } }));

    handleSetWorkflow(prev => {
      const updatedNodes = { ...prev.nodes };
      updatedNodes[newId] = {
        type,
        label: labels[type] || "Node mới",
        depends_on: selectedNodeId ? [selectedNodeId] : [],
        config: defaultConfigs[type] || {}
      };
      return { ...prev, nodes: updatedNodes };
    });

    setSelectedNodeId(newId);
    showToast(`Đã thêm node ${newId} thành công!`);
  };

  const handleDeleteNode = (nodeId) => {
    if (Object.keys(workflow.nodes).length <= 1) {
      showToast("Không thể xóa node cuối cùng. Hệ thống yêu cầu tối thiểu một node.", "warning");
      return;
    }

    handleSetWorkflow(prev => {
      const updatedNodes = { ...prev.nodes };
      delete updatedNodes[nodeId];

      // Dọn dẹp phụ thuộc ở các node khác
      Object.keys(updatedNodes).forEach(k => {
        updatedNodes[k].depends_on = (updatedNodes[k].depends_on || []).filter(dep => dep !== nodeId);
      });

      return { ...prev, nodes: updatedNodes };
    });

    // Cập nhật vị trí node
    setNodePositions(prev => {
      const updated = { ...prev };
      delete updated[nodeId];
      return updated;
    });

    // Chọn ngẫu nhiên một node khác còn lại làm mặc định
    const remainingKeys = Object.keys(workflow.nodes).filter(k => k !== nodeId);
    setSelectedNodeId(remainingKeys[0] || null);
    showToast(`Đã xóa thành công node: ${nodeId}`);
  };

  const handleDuplicateNode = (nodeId) => {
    const srcNode = workflow.nodes[nodeId];
    if (!srcNode) return;

    const dupId = `${nodeId}_copy`;
    const dupNode = JSON.parse(JSON.stringify(srcNode));
    dupNode.label = `${dupNode.label} (Bản sao)`;

    setNodePositions(prev => ({
      ...prev,
      [dupId]: { x: (prev[nodeId]?.x || 100) + 40, y: (prev[nodeId]?.y || 100) + 40 }
    }));

    handleSetWorkflow(prev => {
      const updated = { ...prev.nodes };
      updated[dupId] = dupNode;
      return { ...prev, nodes: updated };
    });

    setSelectedNodeId(dupId);
    showToast(`Đã nhân bản node ${nodeId}`);
  };

  // Thay đổi trạng thái liên kết DAG thủ công bằng cách toggle depend_on
  const handleToggleDependency = (childId, parentId) => {
    if (childId === parentId) return;
    handleSetWorkflow(prev => {
      const updated = { ...prev.nodes };
      const currentDeps = updated[childId].depends_on || [];
      if (currentDeps.includes(parentId)) {
        updated[childId].depends_on = currentDeps.filter(d => d !== parentId);
      } else {
        updated[childId].depends_on = [...currentDeps, parentId];
      }
      return { ...prev, nodes: updated };
    });
  };

  // ==========================================
  // PROPERTY PANEL UPDATERS
  // ==========================================
  const updateNodeMeta = (field, val) => {
    handleSetWorkflow(prev => {
      const updated = { ...prev.nodes };
      if (updated[selectedNodeId]) {
        updated[selectedNodeId] = {
          ...updated[selectedNodeId],
          [field]: val
        };
      }
      return { ...prev, nodes: updated };
    });
  };

  const updateNodeConfig = (path, value) => {
    handleSetWorkflow(prev => {
      const updated = { ...prev.nodes };
      const node = updated[selectedNodeId];
      if (!node) return prev;

      const keys = path.split('.');
      let current = node.config || {};
      node.config = current; // Đảm bảo tham chiếu

      for (let i = 0; i < keys.length - 1; i++) {
        if (!current[keys[i]]) {
          current[keys[i]] = {};
        }
        current = current[keys[i]];
      }
      current[keys[keys.length - 1]] = value;

      return { ...prev, nodes: updated };
    });
  };

  // Thêm/Xóa phần tử trong danh sách Array Fields
  const handleAddArrayField = () => {
    const currentFields = workflow.nodes[selectedNodeId]?.config?.fields || [];
    const newField = { name: `input_field_${currentFields.length + 1}`, type: "STRING" };
    updateNodeConfig("fields", [...currentFields, newField]);
  };

  const handleRemoveArrayField = (idx) => {
    const currentFields = workflow.nodes[selectedNodeId]?.config?.fields || [];
    const filtered = currentFields.filter((_, i) => i !== idx);
    updateNodeConfig("fields", filtered);
  };

  // ==========================================
  // DATA PIPELINE BUILDER ENGINE
  // ==========================================
  const handleAddPipelineOperator = (opType) => {
    const node = workflow.nodes[selectedNodeId];
    if (!node || node.type !== "DATA_PIPELINE") return;

    const currentPipeline = node.config?.pipeline || [];
    
    // Tạo cấu hình mặc định chuẩn cho mỗi loại toán tử
    const defaultOpConfigs = {
      "UNNEST": {
        operator: "UNNEST",
        id: `op_${Date.now().toString().slice(-4)}`,
        inputs: { input_object: "$.steps.node_user_input.output" },
        config: { array_path: "$.items", alias_column: "unnested_item" },
        output_key: `ds_unnested_${currentPipeline.length + 1}`
      },
      "JOIN": {
        operator: "JOIN",
        id: `op_${Date.now().toString().slice(-4)}`,
        inputs: { left_dataset: "$.pipeline_variables.left_ds", right_dataset: "$.steps.some_node.output" },
        config: { join_type: "INNER", on: { left_key: "id", right_key: "id" } },
        output_key: `ds_joined_${currentPipeline.length + 1}`
      },
      "FILTER": {
        operator: "FILTER",
        id: `op_${Date.now().toString().slice(-4)}`,
        inputs: { input_dataset: "$.pipeline_variables.target_ds" },
        config: { condition: "availableQuantity > 0" },
        output_key: `ds_filtered_${currentPipeline.length + 1}`
      },
      "TRANSFORM": {
        operator: "TRANSFORM",
        id: `op_${Date.now().toString().slice(-4)}`,
        inputs: { input_dataset: "$.pipeline_variables.target_ds" },
        config: { operations: { "custom_field": "$.value * 2" } },
        output_key: `ds_transformed_${currentPipeline.length + 1}`
      },
      "AGGREGATE": {
        operator: "AGGREGATE",
        id: `op_${Date.now().toString().slice(-4)}`,
        inputs: { input_dataset: "$.pipeline_variables.target_ds" },
        config: { group_by: ["id"], aggregations: [{ column: "qty", function: "SUM", alias: "sum_qty" }] },
        output_key: `ds_aggregated_${currentPipeline.length + 1}`
      },
      "PIVOT": {
        operator: "PIVOT",
        id: `op_${Date.now().toString().slice(-4)}`,
        inputs: { input_dataset: "$.pipeline_variables.target_ds" },
        config: { index: ["subinventory"], pivot_column: "partNo", value_column: "qty" },
        output_key: `ds_pivoted_${currentPipeline.length + 1}`
      },
      "ACCUMULATE": {
        operator: "ACCUMULATE",
        id: `op_${Date.now().toString().slice(-4)}`,
        inputs: { input_dataset: "$.pipeline_variables.target_ds" },
        config: { accumulator_key: "global_cache:${{tenant_id}}:${{workflow_id}}:custom_accum", merge_strategy: "UPSERT" }
      }
    };

    const newOp = defaultOpConfigs[opType];
    const updatedPipeline = [...currentPipeline, newOp];
    updateNodeConfig("pipeline", updatedPipeline);
    setSelectedPipelineOpIndex(updatedPipeline.length - 1);
    showToast(`Đã bổ sung toán tử [${opType}]`);
  };

  const handleDeletePipelineOperator = (idx) => {
    const node = workflow.nodes[selectedNodeId];
    if (!node) return;
    const currentPipeline = node.config?.pipeline || [];
    const filtered = currentPipeline.filter((_, i) => i !== idx);
    updateNodeConfig("pipeline", filtered);
    setSelectedPipelineOpIndex(Math.max(0, idx - 1));
    showToast("Đã xóa toán tử khỏi Pipeline");
  };

  const handleMovePipelineOperator = (idx, direction) => {
    const node = workflow.nodes[selectedNodeId];
    if (!node) return;
    const pipeline = [...(node.config?.pipeline || [])];
    const targetIdx = idx + direction;
    if (targetIdx < 0 || targetIdx >= pipeline.length) return;

    const temp = pipeline[idx];
    pipeline[idx] = pipeline[targetIdx];
    pipeline[targetIdx] = temp;

    updateNodeConfig("pipeline", pipeline);
    setSelectedPipelineOpIndex(targetIdx);
  };

  const updatePipelineOperatorDetail = (path, value) => {
    const node = workflow.nodes[selectedNodeId];
    if (!node) return;
    const pipeline = [...(node.config?.pipeline || [])];
    const op = { ...pipeline[selectedPipelineOpIndex] };

    const keys = path.split('.');
    let current = op;
    for (let i = 0; i < keys.length - 1; i++) {
      if (!current[keys[i]]) current[keys[i]] = {};
      current = current[keys[i]];
    }
    current[keys[keys.length - 1]] = value;

    pipeline[selectedPipelineOpIndex] = op;
    updateNodeConfig("pipeline", pipeline);
  };

  // ==========================================
  // SCHEMA REGISTRY EDITORS
  // ==========================================
  const handleAddSchemaColumn = (schemaName) => {
    handleSetWorkflow(prev => {
      const registry = { ...prev.schema_registry };
      const schema = { ...registry[schemaName] };
      if (schema.type === "DATASET") {
        const cols = [...(schema.columns || [])];
        cols.push({ name: `new_column_${cols.length + 1}`, type: "STRING", nullable: true });
        schema.columns = cols;
        registry[schemaName] = schema;
      }
      return { ...prev, schema_registry: registry };
    });
  };

  const handleUpdateSchemaColumn = (schemaName, idx, field, val) => {
    handleSetWorkflow(prev => {
      const registry = { ...prev.schema_registry };
      const schema = { ...registry[schemaName] };
      if (schema.type === "DATASET") {
        const cols = [...(schema.columns || [])];
        cols[idx] = { ...cols[idx], [field]: val };
        schema.columns = cols;
        registry[schemaName] = schema;
      }
      return { ...prev, schema_registry: registry };
    });
  };

  const handleRemoveSchemaColumn = (schemaName, idx) => {
    handleSetWorkflow(prev => {
      const registry = { ...prev.schema_registry };
      const schema = { ...registry[schemaName] };
      if (schema.type === "DATASET") {
        schema.columns = schema.columns.filter((_, i) => i !== idx);
        registry[schemaName] = schema;
      }
      return { ...prev, schema_registry: registry };
    });
  };

  // ==========================================
  // EXPRESSION BUILDER ASSISTANT
  // ==========================================
  const applyExpressionToActiveOperator = () => {
    let finalExpr = "";
    if (activeExprTab === "basic") {
      if (exprOperator === "IN") {
        finalExpr = `${exprSelectedColumn} IN ${exprValue}`;
      } else {
        finalExpr = `${exprSelectedColumn} ${exprOperator} ${exprValue}`;
      }
    } else {
      finalExpr = exprValue; // chế độ nâng cao tự điền biểu thức tùy chỉnh
    }

    const node = workflow.nodes[selectedNodeId];
    if (node && node.type === "DATA_PIPELINE") {
      const currentOp = node.config?.pipeline?.[selectedPipelineOpIndex];
      if (currentOp?.operator === "FILTER") {
        updatePipelineOperatorDetail("config.condition", finalExpr);
      } else if (currentOp?.operator === "TRANSFORM") {
        // Áp dụng cho phép biến đổi đầu tiên
        const currentOps = { ...(currentOp.config?.operations || {}) };
        const firstKey = Object.keys(currentOps)[0] || "transformed_column";
        currentOps[firstKey] = finalExpr;
        updatePipelineOperatorDetail("config.operations", currentOps);
      }
      showToast("Đã chèn biểu thức thành công!");
    }
  };

  // ==========================================
  // RENDER CONNECTIONS OVERLAY (SVG)
  // Xây dựng đường nối Bezier mượt mà dựa trên depend_on giữa các Node
  // ==========================================
  const renderSvgOverlay = () => {
    const paths = [];
    const nodes = workflow.nodes || {};

    Object.entries(nodes).forEach(([childId, node]) => {
      const deps = node.depends_on || [];
      deps.forEach(parentId => {
        const parentPos = nodePositions[parentId];
        const childPos = nodePositions[childId];

        if (parentPos && childPos) {
          // Điểm ra: chính giữa sườn phải của node cha
          const startX = parentPos.x + 230; 
          const startY = parentPos.y + 40;
          // Điểm vào: chính giữa sườn trái của node con
          const endX = childPos.x;
          const endY = childPos.y + 40;

          // Tạo đường cong Bezier mềm mại kiểu React Flow
          const controlDist = Math.abs(endX - startX) * 0.5;
          const d = `M ${startX} ${startY} C ${startX + controlDist} ${startY}, ${endX - controlDist} ${endY}, ${endX} ${endY}`;

          paths.push(
            <g key={`${parentId}-${childId}`} className="group">
              {/* Đường viền đệm click rộng hơn để tăng UX tương tác */}
              <path
                d={d}
                fill="none"
                stroke="transparent"
                strokeWidth="12"
                className="cursor-pointer"
                onClick={() => handleToggleDependency(childId, parentId)}
                title="Nhấp để xóa đường liên kết phụ thuộc"
              />
              {/* Đường hiển thị chính */}
              <path
                d={d}
                fill="none"
                stroke="#6366f1"
                strokeWidth="2.5"
                strokeDasharray="4 2"
                className="transition-all duration-300 group-hover:stroke-rose-500 group-hover:stroke-[3.5px] cursor-pointer"
                onClick={() => handleToggleDependency(childId, parentId)}
              />
              <circle cx={startX} cy={startY} r="4.5" fill="#a5b4fc" />
              <circle cx={endX} cy={endY} r="4.5" fill="#6366f1" />
            </g>
          );
        }
      });
    });

    return (
      <svg id="svg-overlay" className="absolute top-0 left-0 w-full h-full pointer-events-auto">
        <defs>
          <pattern id="dot-grid" width="20" height="20" patternUnits="userSpaceOnUse">
            <circle cx="2" cy="2" r="1.2" fill="#334155" />
          </pattern>
        </defs>
        <rect width="100%" height="100%" fill="url(#dot-grid)" />
        {paths}
      </svg>
    );
  };

  // Trích xuất cây Context Path Browser để copy/paste
  const contextPathsTree = [
    { label: "Môi trường (env_mappings)", path: "${{context.env_mappings.PROD.ems_host}}" },
    { label: "Định danh người dùng mặc định", path: "${{context.global.variables.default_user_id}}" },
    { label: "Khóa bảo mật API key", path: "${{context.global.secrets.ems_api_key}}" },
    { label: "Phần tử lặp hiện tại", path: "${{loop_item}}" },
    { label: "Dữ liệu nhập từ UI", path: "$.steps.node_user_input.output" },
    { label: "Danh sách vật tư thô đầu ra", path: "$.steps.node_fetch_ems_data.output" },
    { label: "Báo cáo PIVOT hoàn thiện", path: "$.steps.node_data_pipeline.pipeline_output.ds_pivoted_report" }
  ];

  // Sao chép nhanh JSONPath vào Clipboard
  const copyToClipboard = (text) => {
    const el = document.createElement('textarea');
    el.value = text;
    document.body.appendChild(el);
    el.select();
    document.execCommand('copy');
    document.body.removeChild(el);
    showToast(`Đã sao chép: ${text}`);
  };

  return (
    <div className="min-h-screen bg-[#0f172a] text-slate-100 flex flex-col font-sans select-none antialiased">
      
      {/* ==========================================
          HEADER BAR
          ========================================== */}
      <header className="bg-[#0b0f19] border-b border-slate-800 px-6 py-4 flex items-center justify-between shadow-lg sticky top-0 z-40">
        <div className="flex items-center space-x-3">
          <div className="p-2 bg-gradient-to-tr from-indigo-600 to-violet-500 rounded-lg text-white shadow-md shadow-indigo-950/40">
            <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10" />
            </svg>
          </div>
          <div>
            <div className="flex items-center space-x-2">
              <h1 className="text-lg font-bold tracking-wide text-slate-100">Enterprise Workflow & Data Pipeline Engine</h1>
              <span className="px-2 py-0.5 text-xs font-semibold bg-indigo-950 text-indigo-300 rounded border border-indigo-800/60">Spec 9.0.0</span>
            </div>
            <p className="text-xs text-slate-400">Thiết kế, giám sát, và phát sinh mã nguồn đồ thị DAG & Vectorized Pipeline</p>
          </div>
        </div>

        {/* Thao tác Global */}
        <div className="flex items-center space-x-3">
          <div className="flex bg-[#161f38] p-1 rounded-lg border border-slate-800">
            <button
              onClick={() => { setActiveTab("canvas"); }}
              className={`px-4 py-1.5 rounded-md text-xs font-medium transition-all ${activeTab === 'canvas' ? 'bg-indigo-600 text-white shadow' : 'text-slate-400 hover:text-white'}`}
            >
              Visual DAG Canvas
            </button>
            <button
              onClick={() => { 
                setActiveTab("pipeline"); 
                // Tự động nhảy sang node_data_pipeline nếu có
                if (workflow.nodes?.node_data_pipeline) setSelectedNodeId("node_data_pipeline");
              }}
              className={`px-4 py-1.5 rounded-md text-xs font-medium transition-all ${activeTab === 'pipeline' ? 'bg-indigo-600 text-white shadow' : 'text-slate-400 hover:text-white'}`}
            >
              Pipeline Vectorized
            </button>
            <button
              onClick={() => { setActiveTab("schemas"); }}
              className={`px-4 py-1.5 rounded-md text-xs font-medium transition-all ${activeTab === 'schemas' ? 'bg-indigo-600 text-white shadow' : 'text-slate-400 hover:text-white'}`}
            >
              Schema Registry
            </button>
            <button
              onClick={() => { setActiveTab("context"); }}
              className={`px-4 py-1.5 rounded-md text-xs font-medium transition-all ${activeTab === 'context' ? 'bg-indigo-600 text-white shadow' : 'text-slate-400 hover:text-white'}`}
            >
              Context (Biến/Môi trường)
            </button>
          </div>

          <div className="h-6 w-[1px] bg-slate-800" />

          {/* Undo, Redo, Reset */}
          <div className="flex items-center space-x-1">
            <button
              onClick={handleUndo}
              disabled={historyPointer <= 0}
              className="p-1.5 bg-[#161f38] hover:bg-slate-800 text-slate-300 disabled:opacity-40 rounded border border-slate-850"
              title="Undo bước trước"
            >
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M12.066 11.2a1 1 0 000 1.6l5.334 4A1 1 0 0019 16V8a1 1 0 00-1.6-.8l-5.334 4z" /><path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M4.066 11.2a1 1 0 000 1.6l5.334 4A1 1 0 0011 16V8a1 1 0 00-1.6-.8l-5.334 4z" /></svg>
            </button>
            <button
              onClick={handleRedo}
              disabled={historyPointer >= history.length - 1}
              className="p-1.5 bg-[#161f38] hover:bg-slate-800 text-slate-300 disabled:opacity-40 rounded border border-slate-850"
              title="Redo bước tiếp theo"
            >
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M11.934 12.8a1 1 0 000-1.6l-5.334-4A1 1 0 005 8v8a1 1 0 001.6.8l5.334-4z" /><path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M19.934 12.8a1 1 0 000-1.6l-5.334-4A1 1 0 0013 8v8a1 1 0 001.6.8l5.334-4z" /></svg>
            </button>
            <button
              onClick={() => {
                if (confirm("Bạn có chắc chắn muốn khôi phục thiết kế ban đầu? Mọi chỉnh sửa chưa lưu sẽ bị ghi đè.")) {
                  setWorkflow(INITIAL_WORKFLOW_SPEC);
                  setNodePositions(INITIAL_NODE_POSITIONS);
                  setHistory([JSON.parse(JSON.stringify(INITIAL_WORKFLOW_SPEC))]);
                  setHistoryPointer(0);
                  showToast("Đã khôi phục thiết kế mẫu ban đầu.");
                }
              }}
              className="px-2.5 py-1.5 bg-rose-950/40 hover:bg-rose-900/50 border border-rose-800 text-rose-200 text-xs font-semibold rounded"
            >
              Khởi động lại mẫu
            </button>
          </div>
        </div>
      </header>

      {/* Toast Notification */}
      {toast && (
        <div className="fixed bottom-4 right-4 bg-slate-900 border border-indigo-500 shadow-xl shadow-indigo-950/40 text-slate-200 px-4 py-3 rounded-lg flex items-center space-x-3 z-50 animate-bounce">
          <span className="flex h-2.5 w-2.5 relative">
            <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
            <span className="relative inline-flex rounded-full h-2.5 w-2.5 bg-emerald-500"></span>
          </span>
          <p className="text-sm font-semibold">{toast.message}</p>
        </div>
      )}

      {/* ==========================================
          CHỦ THỂ HOẠT ĐỘNG CHÍNH (MAIN WORKSPACE)
          ========================================== */}
      <div className="flex-1 flex overflow-hidden">
        
        {/* SIDEBAR TRÁI: KHÔNG GIAN KHÁM PHÁ JSONPATH & CHỌN CÔNG CỤ NHANH */}
        <aside className="w-80 bg-[#0b0f19] border-r border-slate-800 flex flex-col overflow-y-auto p-4 shrink-0 space-y-6">
          
          {/* Metadata Tầng Cao Nhất */}
          <div>
            <h3 className="text-xs font-bold text-slate-400 uppercase tracking-widest mb-3">Thông tin Workflow</h3>
            <div className="space-y-3 bg-[#111625] p-3 rounded-lg border border-slate-800/80">
              <div>
                <label className="block text-[10px] text-slate-500 font-semibold uppercase">Định danh (Workflow ID)</label>
                <input
                  type="text"
                  value={workflow.workflow_id}
                  onChange={(e) => handleSetWorkflow({ ...workflow, workflow_id: e.target.value })}
                  className="w-full bg-[#070a13] border border-slate-800 rounded px-2 py-1 text-xs text-indigo-400 font-mono mt-1"
                />
              </div>
              <div>
                <label className="block text-[10px] text-slate-500 font-semibold uppercase">Tên hiển thị</label>
                <input
                  type="text"
                  value={workflow.name}
                  onChange={(e) => handleSetWorkflow({ ...workflow, name: e.target.value })}
                  className="w-full bg-[#070a13] border border-slate-800 rounded px-2 py-1 text-xs text-slate-200 mt-1"
                />
              </div>
              <div>
                <label className="block text-[10px] text-slate-500 font-semibold uppercase">Đối tượng phân quyền (Tenant ID)</label>
                <input
                  type="text"
                  value={workflow.tenant_id}
                  onChange={(e) => handleSetWorkflow({ ...workflow, tenant_id: e.target.value })}
                  className="w-full bg-[#070a13] border border-slate-800 rounded px-2 py-1 text-xs text-slate-400 font-mono mt-1"
                />
              </div>
            </div>
          </div>

          {/* Quick Creator Tools */}
          <div>
            <h3 className="text-xs font-bold text-slate-400 uppercase tracking-widest mb-3">Thêm Node Chức Năng</h3>
            <div className="grid grid-cols-1 gap-2">
              <button
                onClick={() => handleAddNode("USER_INTERACTION")}
                className="flex items-center space-x-2.5 p-2 bg-[#161f38] hover:bg-slate-800 border border-slate-800 rounded text-left group transition"
              >
                <span className="w-2.5 h-2.5 rounded-full bg-cyan-400" />
                <div className="text-xs">
                  <span className="font-semibold block text-slate-200">User Input Node</span>
                  <span className="text-[10px] text-slate-400">Thiết lập giao diện form nhập</span>
                </div>
              </button>

              <button
                onClick={() => handleAddNode("CONNECTOR_HTTP")}
                className="flex items-center space-x-2.5 p-2 bg-[#161f38] hover:bg-slate-800 border border-slate-800 rounded text-left group transition"
              >
                <span className="w-2.5 h-2.5 rounded-full bg-emerald-400" />
                <div className="text-xs">
                  <span className="font-semibold block text-slate-200">Connector HTTP</span>
                  <span className="text-[10px] text-slate-400">Gọi API ngoài/Mảng lặp</span>
                </div>
              </button>

              <button
                onClick={() => handleAddNode("DATA_PIPELINE")}
                className="flex items-center space-x-2.5 p-2 bg-[#161f38] hover:bg-slate-800 border border-slate-800 rounded text-left group transition"
              >
                <span className="w-2.5 h-2.5 rounded-full bg-indigo-400" />
                <div className="text-xs">
                  <span className="font-semibold block text-slate-200">Vectorized Pipeline</span>
                  <span className="text-[10px] text-slate-400">Toán tử biến đổi quan hệ</span>
                </div>
              </button>

              <button
                onClick={() => handleAddNode("USER_DISPLAY")}
                className="flex items-center space-x-2.5 p-2 bg-[#161f38] hover:bg-slate-800 border border-slate-800 rounded text-left group transition"
              >
                <span className="w-2.5 h-2.5 rounded-full bg-amber-400" />
                <div className="text-xs">
                  <span className="font-semibold block text-slate-200">User Display</span>
                  <span className="text-[10px] text-slate-400">Hiển thị đồ họa dữ liệu</span>
                </div>
              </button>
            </div>
          </div>

          {/* JSONPath Browser - Cực kỳ hữu dụng khi build Expression */}
          <div>
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-xs font-bold text-slate-400 uppercase tracking-widest">JSONPath Browser</h3>
              <span className="text-[9px] text-indigo-400 italic">Bấm để sao chép</span>
            </div>
            <div className="bg-[#111625] rounded-lg border border-slate-800 p-2 divide-y divide-slate-850">
              {contextPathsTree.map((item, idx) => (
                <div
                  key={idx}
                  onClick={() => copyToClipboard(item.path)}
                  className="py-1.5 px-2 hover:bg-[#1c243f] rounded cursor-pointer transition text-xs flex justify-between items-center group"
                >
                  <div>
                    <span className="text-slate-400 font-medium block text-[11px]">{item.label}</span>
                    <span className="text-indigo-400 font-mono text-[10px]">{item.path}</span>
                  </div>
                  <svg className="w-3.5 h-3.5 text-slate-600 group-hover:text-indigo-400 opacity-0 group-hover:opacity-100 transition" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M8 5H6a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2v-1M8 5a2 2 0 002 2h2a2 2 0 002-2M8 5a2 2 0 012-2h2a2 2 0 012 2m0 0h2a2 2 0 012 2v3m2 4H10m0 0l3-3m-3 3l3 3" />
                  </svg>
                </div>
              ))}
            </div>
          </div>

          {/* Sơ đồ trạng thái Validation thời gian thực */}
          <div>
            <h3 className="text-xs font-bold text-slate-400 uppercase tracking-widest mb-3">Thông điệp Hệ Thống</h3>
            <div className="space-y-2 max-h-56 overflow-y-auto">
              {validationErrors.length === 0 ? (
                <div className="p-3 bg-emerald-950/25 border border-emerald-800/60 rounded text-emerald-300 text-xs flex items-center space-x-2">
                  <svg className="w-4.5 h-4.5 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>
                  <span>Cấu hình hoàn toàn hợp lệ! Sẵn sàng biên dịch v9.0.0.</span>
                </div>
              ) : (
                validationErrors.map((err, idx) => (
                  <div
                    key={idx}
                    className={`p-2.5 rounded border text-xs space-y-1 ${
                      err.severity === "CRITICAL" || err.severity === "HIGH"
                        ? "bg-rose-950/30 border-rose-800/80 text-rose-300"
                        : "bg-amber-950/30 border-amber-800/80 text-amber-300"
                    }`}
                  >
                    <div className="flex items-center justify-between font-bold">
                      <span className="uppercase text-[10px] tracking-wider">[{err.category}]</span>
                      <span className="text-[9px] px-1 bg-black/40 rounded text-rose-400">{err.severity}</span>
                    </div>
                    <p className="text-[11px] leading-relaxed">{err.message}</p>
                  </div>
                ))
              )}
            </div>
          </div>
        </aside>

        {/* ==========================================
            KHÔNG GIAN LÀM VIỆC CHÍNH (DYNAMIC CANVAS / SCHEMA TABS)
            ========================================== */}
        <main className="flex-1 flex flex-col bg-[#0f172a] relative overflow-hidden">
          
          {/* TAB 1: VISUAL DAG CANVAS DESIGNER */}
          {activeTab === "canvas" && (
            <div className="flex-1 flex flex-col relative overflow-hidden">
              
              {/* Controls Zoom/Pan thu nhỏ đầu trang Canvas */}
              <div className="absolute top-4 left-4 z-20 flex items-center space-x-2 bg-slate-900/90 backdrop-blur border border-slate-800 p-1.5 rounded-lg shadow-lg">
                <button
                  onClick={() => setCanvasScale(Math.max(0.6, canvasScale - 0.1))}
                  className="p-1 hover:bg-slate-800 rounded text-slate-300 transition"
                  title="Thu nhỏ"
                >
                  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M20 12H4" /></svg>
                </button>
                <span className="text-xs font-mono px-2 text-slate-300">{Math.round(canvasScale * 100)}%</span>
                <button
                  onClick={() => setCanvasScale(Math.min(1.5, canvasScale + 0.1))}
                  className="p-1 hover:bg-slate-800 rounded text-slate-300 transition"
                  title="Phóng to"
                >
                  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M12 4v16m8-8H4" /></svg>
                </button>
                <button
                  onClick={() => { setCanvasScale(1.0); setNodePositions(INITIAL_NODE_POSITIONS); }}
                  className="px-2 py-0.5 hover:bg-slate-800 text-[10px] text-slate-300 rounded transition border border-slate-700"
                >
                  Mặc định
                </button>
              </div>

              {/* Hướng dẫn nhanh Canvas */}
              <div className="absolute top-4 right-4 z-20 text-[10px] bg-slate-900/85 backdrop-blur border border-slate-800 px-3 py-2 rounded-lg text-slate-400 space-y-1 shadow">
                <p className="font-semibold text-indigo-400">💡 Hướng dẫn thiết kế:</p>
                <p>• Kéo rê tiêu đề Card để thay đổi vị trí tự do.</p>
                <p>• Click để chỉnh sửa thuộc tính bên bảng phải.</p>
                <p>• Double click node cha & node con để tạo/xóa liên kết phụ thuộc.</p>
              </div>

              {/* Vùng chứa Canvas Drag & Drop */}
              <div
                ref={canvasRef}
                onMouseDown={handleCanvasMouseDown}
                onMouseMove={handleCanvasMouseMove}
                onMouseUp={handleCanvasMouseUp}
                className="flex-1 relative overflow-auto cursor-grab active:cursor-grabbing select-none"
                style={{
                  transform: `scale(${canvasScale})`,
                  transformOrigin: "top left",
                  width: "100%",
                  height: "100%",
                  minWidth: "1800px",
                  minHeight: "1000px"
                }}
              >
                {/* SVG kết nối dòng phụ thuộc phía nền */}
                {renderSvgOverlay()}

                {/* Danh sách Node Cards */}
                {Object.entries(workflow.nodes || {}).map(([id, node]) => {
                  const pos = nodePositions[id] || { x: 100, y: 100 };
                  const isSelected = selectedNodeId === id;

                  // Mã màu tiêu đề theo loại node
                  const headerBg = {
                    "USER_INTERACTION": "bg-cyan-500/10 text-cyan-400 border-cyan-500/25",
                    "CONNECTOR_HTTP": "bg-emerald-500/10 text-emerald-400 border-emerald-500/25",
                    "DATA_PIPELINE": "bg-indigo-500/10 text-indigo-400 border-indigo-500/25",
                    "USER_DISPLAY": "bg-amber-500/10 text-amber-400 border-amber-500/25"
                  }[node.type] || "bg-slate-500/10 text-slate-300";

                  const dotColor = {
                    "USER_INTERACTION": "bg-cyan-400",
                    "CONNECTOR_HTTP": "bg-emerald-400",
                    "DATA_PIPELINE": "bg-indigo-400",
                    "USER_DISPLAY": "bg-amber-400"
                  }[node.type] || "bg-slate-400";

                  return (
                    <div
                      key={id}
                      style={{ left: pos.x, top: pos.y }}
                      onClick={(e) => { e.stopPropagation(); setSelectedNodeId(id); }}
                      onDoubleClick={() => {
                        if (selectedNodeId && selectedNodeId !== id) {
                          handleToggleDependency(id, selectedNodeId);
                        }
                      }}
                      className={`absolute w-64 bg-slate-900 border-2 rounded-xl shadow-2xl transition-all duration-200 cursor-pointer ${
                        isSelected ? "border-violet-500 shadow-violet-950/40 scale-[1.03] z-30" : getNodeSeverity(id)
                      }`}
                    >
                      {/* Node Header & Drag handle */}
                      <div
                        onMouseDown={(e) => handleNodeDragStart(e, id)}
                        className={`px-3 py-2.5 rounded-t-xl border-b flex items-center justify-between ${headerBg}`}
                      >
                        <div className="flex items-center space-x-2 truncate">
                          <span className={`w-2 h-2 rounded-full ${dotColor}`} />
                          <span className="font-bold text-xs truncate">{node.label}</span>
                        </div>
                        <span className="text-[9px] font-mono px-1.5 py-0.5 bg-black/40 rounded">
                          {node.type.slice(0, 10)}
                        </span>
                      </div>

                      {/* Node Body */}
                      <div className="p-3 text-xs space-y-2">
                        <div>
                          <span className="text-[10px] text-slate-500 uppercase block font-semibold">Định danh Node</span>
                          <span className="font-mono text-indigo-400 font-medium">{id}</span>
                        </div>

                        {node.depends_on && node.depends_on.length > 0 && (
                          <div>
                            <span className="text-[10px] text-slate-500 uppercase block font-semibold">Phụ thuộc vào</span>
                            <div className="flex flex-wrap gap-1 mt-1">
                              {node.depends_on.map(dep => (
                                <span key={dep} className="px-1.5 py-0.5 bg-slate-800 text-slate-400 rounded text-[9px] font-mono border border-slate-700">
                                  {dep}
                                </span>
                              ))}
                            </div>
                          </div>
                        )}

                        {node.config?.output_schema && (
                          <div className="flex items-center justify-between pt-1 border-t border-slate-800/60">
                            <span className="text-[10px] text-slate-500 font-semibold">Schema Output</span>
                            <span className="text-[10px] text-indigo-400 font-mono font-medium">{node.config.output_schema}</span>
                          </div>
                        )}

                        {node.type === "DATA_PIPELINE" && (
                          <div className="flex items-center justify-between pt-1 border-t border-slate-800/60">
                            <span className="text-[10px] text-slate-500 font-semibold">Toán tử xử lý</span>
                            <span className="px-1.5 py-0.5 bg-indigo-950 text-indigo-300 rounded text-[10px] font-bold">
                              {node.config?.pipeline?.length || 0} Operators
                            </span>
                          </div>
                        )}
                      </div>

                      {/* Action Trực Tiếp Trên Node */}
                      <div className="px-3 py-2 bg-slate-950/65 rounded-b-xl border-t border-slate-800 flex justify-end space-x-1">
                        <button
                          onClick={(e) => { e.stopPropagation(); handleDuplicateNode(id); }}
                          className="p-1 hover:bg-slate-800 rounded text-slate-400 hover:text-indigo-400 transition"
                          title="Nhân bản node"
                        >
                          <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M8 7v8a2 2 0 002 2h6M8 7V5a2 2 0 012-2h4.586a1 1 0 01.707.293l4.414 4.414a1 1 0 01.293.707V15a2 2 0 01-2 2h-2M8 7H6a2 2 0 00-2 2v10a2 2 0 002 2h8a2 2 0 002-2v-2" /></svg>
                        </button>
                        <button
                          onClick={(e) => { e.stopPropagation(); handleDeleteNode(id); }}
                          className="p-1 hover:bg-slate-800 rounded text-slate-400 hover:text-rose-500 transition"
                          title="Xóa node"
                        >
                          <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" /></svg>
                        </button>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          {/* TAB 2: PIPELINE VECTORIZED SEQUENTIAL DESIGNER */}
          {activeTab === "pipeline" && (
            <div className="flex-1 flex flex-col p-6 overflow-y-auto space-y-6">
              <div className="flex items-center justify-between pb-4 border-b border-slate-800">
                <div>
                  <h2 className="text-xl font-bold text-slate-200">Vectorized Pipeline Editor</h2>
                  <p className="text-xs text-slate-400">Thiết kế trình tự xử lý đại số quan hệ tối ưu hóa ngay trên bộ nhớ (In-Memory)</p>
                </div>
                {/* Node Target Selector */}
                <div className="flex items-center space-x-2">
                  <span className="text-xs text-slate-400">Node Pipeline hoạt động:</span>
                  <select
                    value={selectedNodeId}
                    onChange={(e) => setSelectedNodeId(e.target.value)}
                    className="bg-slate-900 border border-slate-700 rounded px-2 py-1 text-xs text-indigo-400 font-mono"
                  >
                    {Object.entries(workflow.nodes || {})
                      .filter(([_, n]) => n.type === "DATA_PIPELINE")
                      .map(([id]) => (
                        <option key={id} value={id}>{id}</option>
                      ))}
                  </select>
                </div>
              </div>

              {/* Nếu không có node pipeline nào */}
              {(!selectedNodeId || workflow.nodes[selectedNodeId]?.type !== "DATA_PIPELINE") ? (
                <div className="p-8 text-center bg-slate-900/40 rounded-xl border border-dashed border-slate-800 text-slate-400">
                  Vui lòng tạo hoặc chọn một Node có kiểu [DATA_PIPELINE] để chỉnh sửa các bước biến đổi dữ liệu.
                </div>
              ) : (
                <div className="grid grid-cols-12 gap-6 items-start">
                  
                  {/* Cột trái: Danh sách các Operator theo luồng tuyến tính */}
                  <div className="col-span-5 bg-[#0b0f19] border border-slate-800 rounded-xl p-4 space-y-4">
                    <div className="flex items-center justify-between">
                      <span className="text-xs font-bold text-slate-400 uppercase tracking-wider">Trình tự luồng dữ liệu</span>
                      <span className="px-2 py-0.5 bg-indigo-950 text-indigo-300 text-[10px] rounded font-mono font-bold">
                        {workflow.nodes[selectedNodeId]?.config?.pipeline?.length || 0} bước
                      </span>
                    </div>

                    {/* Bộ điều khiển thêm nhanh Operator */}
                    <div className="grid grid-cols-3 gap-1.5">
                      {["UNNEST", "JOIN", "FILTER", "TRANSFORM", "AGGREGATE", "PIVOT", "ACCUMULATE"].map(op => (
                        <button
                          key={op}
                          onClick={() => handleAddPipelineOperator(op)}
                          className="px-2 py-1 bg-[#161f38] hover:bg-[#202c4e] text-slate-200 border border-slate-800 rounded text-[10px] font-bold text-center transition"
                        >
                          + {op}
                        </button>
                      ))}
                    </div>

                    {/* Danh sách các toán tử hiện hành */}
                    <div className="space-y-2 max-h-[500px] overflow-y-auto pr-1">
                      {(workflow.nodes[selectedNodeId]?.config?.pipeline || []).length === 0 ? (
                        <div className="p-6 text-center text-xs text-slate-500 italic">
                          Chưa có toán tử nào. Chọn thêm phía trên để bắt đầu tích hợp.
                        </div>
                      ) : (
                        (workflow.nodes[selectedNodeId]?.config?.pipeline || []).map((op, idx) => {
                          const isActive = selectedPipelineOpIndex === idx;
                          return (
                            <div
                              key={idx}
                              onClick={() => setSelectedPipelineOpIndex(idx)}
                              className={`p-3 rounded-lg border cursor-pointer transition flex items-center justify-between ${
                                isActive ? "bg-indigo-950/40 border-indigo-500/80 text-white" : "bg-[#111625] border-slate-800 text-slate-400 hover:text-slate-200"
                              }`}
                            >
                              <div className="flex items-center space-x-2.5 truncate">
                                <span className="text-[10px] font-mono font-bold bg-slate-800 px-1.5 py-0.5 rounded text-slate-400">
                                  #{idx + 1}
                                </span>
                                <div>
                                  <span className="font-bold text-xs block text-slate-200">{op.operator}</span>
                                  <span className="text-[9px] font-mono text-slate-500 block truncate">
                                    out: {op.output_key || "ACCUMULATOR"}
                                  </span>
                                </div>
                              </div>

                              {/* Điều chỉnh vị trí */}
                              <div className="flex items-center space-x-1" onClick={(e) => e.stopPropagation()}>
                                <button
                                  onClick={() => handleMovePipelineOperator(idx, -1)}
                                  disabled={idx === 0}
                                  className="p-1 hover:bg-slate-800 rounded text-slate-400 disabled:opacity-30"
                                >
                                  ▲
                                </button>
                                <button
                                  onClick={() => handleMovePipelineOperator(idx, 1)}
                                  disabled={idx === (workflow.nodes[selectedNodeId]?.config?.pipeline || []).length - 1}
                                  className="p-1 hover:bg-slate-800 rounded text-slate-400 disabled:opacity-30"
                                >
                                  ▼
                                </button>
                                <button
                                  onClick={() => handleDeletePipelineOperator(idx)}
                                  className="p-1 hover:bg-slate-800 rounded text-rose-400"
                                >
                                  ✕
                                </button>
                              </div>
                            </div>
                          );
                        })
                      )}
                    </div>
                  </div>

                  {/* Cột phải: Cấu hình chi tiết Operator đang được chọn */}
                  <div className="col-span-7 bg-[#0b0f19] border border-slate-800 rounded-xl p-6 space-y-6">
                    {selectedPipelineOpIndex < (workflow.nodes[selectedNodeId]?.config?.pipeline || []).length ? (
                      (() => {
                        const activeOp = workflow.nodes[selectedNodeId]?.config?.pipeline?.[selectedPipelineOpIndex];
                        if (!activeOp) return null;
                        return (
                          <div className="space-y-5">
                            <div className="flex items-center justify-between pb-3 border-b border-slate-800">
                              <div>
                                <h3 className="text-sm font-bold text-slate-200">Toán tử: {activeOp.operator}</h3>
                                <p className="text-[11px] text-slate-400">Cấu hình tham số và đường dữ liệu vào/ra</p>
                              </div>
                              <span className="text-xs font-mono text-indigo-400">bước #{selectedPipelineOpIndex + 1}</span>
                            </div>

                            {/* Cấu hình Keys */}
                            <div className="grid grid-cols-2 gap-4">
                              <div>
                                <label className="block text-[10px] text-slate-500 font-semibold uppercase">Đầu Ra Dataset Key</label>
                                {activeOp.operator !== "ACCUMULATE" ? (
                                  <input
                                    type="text"
                                    value={activeOp.output_key || ""}
                                    onChange={(e) => updatePipelineOperatorDetail("output_key", e.target.value)}
                                    className="w-full bg-[#111625] border border-slate-800 rounded px-2 py-1.5 text-xs text-indigo-400 font-mono mt-1"
                                  />
                                ) : (
                                  <span className="block text-xs text-slate-500 italic mt-2">N/A (Lưu Global Cache)</span>
                                )}
                              </div>
                              <div>
                                <label className="block text-[10px] text-slate-500 font-semibold uppercase">ID Thuật Toán</label>
                                <input
                                  type="text"
                                  value={activeOp.id || ""}
                                  onChange={(e) => updatePipelineOperatorDetail("id", e.target.value)}
                                  className="w-full bg-[#111625] border border-slate-800 rounded px-2 py-1.5 text-xs font-mono text-slate-300 mt-1"
                                />
                              </div>
                            </div>

                            {/* Trường cấu hình Động dựa trên loại Operator */}
                            <div className="bg-[#111625] p-4 rounded-lg border border-slate-800 space-y-4">
                              <span className="text-xs font-bold text-slate-300 block border-b border-slate-800/60 pb-1">
                                Thuộc tính Toán tử
                              </span>

                              {/* Operator: UNNEST */}
                              {activeOp.operator === "UNNEST" && (
                                <div className="space-y-3">
                                  <div>
                                    <label className="text-[10px] text-slate-400 block font-semibold">Đường dẫn Object nguồn (input_object)</label>
                                    <input
                                      type="text"
                                      value={activeOp.inputs?.input_object || ""}
                                      onChange={(e) => updatePipelineOperatorDetail("inputs.input_object", e.target.value)}
                                      className="w-full bg-[#070a13] border border-slate-800 rounded px-2.5 py-1.5 text-xs text-indigo-400 font-mono mt-1"
                                    />
                                  </div>
                                  <div className="grid grid-cols-2 gap-3">
                                    <div>
                                      <label className="text-[10px] text-slate-400 block font-semibold">Đường dẫn mảng lặp (array_path)</label>
                                      <input
                                        type="text"
                                        value={activeOp.config?.array_path || ""}
                                        onChange={(e) => updatePipelineOperatorDetail("config.array_path", e.target.value)}
                                        className="w-full bg-[#070a13] border border-slate-800 rounded px-2.5 py-1.5 text-xs text-slate-300 font-mono mt-1"
                                      />
                                    </div>
                                    <div>
                                      <label className="text-[10px] text-slate-400 block font-semibold">Bí danh cột unnest (alias_column)</label>
                                      <input
                                        type="text"
                                        value={activeOp.config?.alias_column || ""}
                                        onChange={(e) => updatePipelineOperatorDetail("config.alias_column", e.target.value)}
                                        className="w-full bg-[#070a13] border border-slate-800 rounded px-2.5 py-1.5 text-xs text-slate-300 mt-1"
                                      />
                                    </div>
                                  </div>
                                </div>
                              )}

                              {/* Operator: JOIN */}
                              {activeOp.operator === "JOIN" && (
                                <div className="space-y-3">
                                  <div className="grid grid-cols-2 gap-3">
                                    <div>
                                      <label className="text-[10px] text-slate-400 block font-semibold">Dataset Trái (Left)</label>
                                      <input
                                        type="text"
                                        value={activeOp.inputs?.left_dataset || ""}
                                        onChange={(e) => updatePipelineOperatorDetail("inputs.left_dataset", e.target.value)}
                                        className="w-full bg-[#070a13] border border-slate-800 rounded px-2.5 py-1.5 text-xs text-indigo-400 font-mono mt-1"
                                      />
                                    </div>
                                    <div>
                                      <label className="text-[10px] text-slate-400 block font-semibold">Dataset Phải (Right)</label>
                                      <input
                                        type="text"
                                        value={activeOp.inputs?.right_dataset || ""}
                                        onChange={(e) => updatePipelineOperatorDetail("inputs.right_dataset", e.target.value)}
                                        className="w-full bg-[#070a13] border border-slate-800 rounded px-2.5 py-1.5 text-xs text-indigo-400 font-mono mt-1"
                                      />
                                    </div>
                                  </div>
                                  <div className="grid grid-cols-3 gap-2">
                                    <div>
                                      <label className="text-[10px] text-slate-400 block font-semibold">Loại Join</label>
                                      <select
                                        value={activeOp.config?.join_type || "INNER"}
                                        onChange={(e) => updatePipelineOperatorDetail("config.join_type", e.target.value)}
                                        className="w-full bg-[#070a13] border border-slate-800 rounded px-2.5 py-1.5 text-xs text-slate-300 mt-1"
                                      >
                                        <option value="INNER">INNER JOIN</option>
                                        <option value="LEFT">LEFT OUTER JOIN</option>
                                        <option value="RIGHT">RIGHT OUTER JOIN</option>
                                        <option value="FULL">FULL JOIN</option>
                                      </select>
                                    </div>
                                    <div>
                                      <label className="text-[10px] text-slate-400 block font-semibold">Key liên kết Trái</label>
                                      <input
                                        type="text"
                                        value={activeOp.config?.on?.left_key || ""}
                                        onChange={(e) => updatePipelineOperatorDetail("config.on.left_key", e.target.value)}
                                        className="w-full bg-[#070a13] border border-slate-800 rounded px-2.5 py-1.5 text-xs text-slate-300 font-mono mt-1"
                                      />
                                    </div>
                                    <div>
                                      <label className="text-[10px] text-slate-400 block font-semibold">Key liên kết Phải</label>
                                      <input
                                        type="text"
                                        value={activeOp.config?.on?.right_key || ""}
                                        onChange={(e) => updatePipelineOperatorDetail("config.on.right_key", e.target.value)}
                                        className="w-full bg-[#070a13] border border-slate-800 rounded px-2.5 py-1.5 text-xs text-slate-300 font-mono mt-1"
                                      />
                                    </div>
                                  </div>
                                </div>
                              )}

                              {/* Operator: FILTER */}
                              {activeOp.operator === "FILTER" && (
                                <div className="space-y-4">
                                  <div>
                                    <label className="text-[10px] text-slate-400 block font-semibold">Dataset Nguồn (input_dataset)</label>
                                    <input
                                      type="text"
                                      value={activeOp.inputs?.input_dataset || ""}
                                      onChange={(e) => updatePipelineOperatorDetail("inputs.input_dataset", e.target.value)}
                                      className="w-full bg-[#070a13] border border-slate-800 rounded px-2.5 py-1.5 text-xs text-indigo-400 font-mono mt-1"
                                    />
                                  </div>

                                  {/* Tích hợp trực tiếp bộ sinh Expression Builder */}
                                  <div className="p-3.5 bg-slate-900 rounded-lg border border-slate-800 space-y-3">
                                    <div className="flex items-center justify-between">
                                      <span className="text-[11px] font-bold text-slate-300 uppercase">Trình trợ giúp xây dựng biểu thức</span>
                                      <div className="flex space-x-2">
                                        <button
                                          onClick={() => setActiveExprTab("basic")}
                                          className={`px-2 py-0.5 text-[10px] rounded ${activeExprTab === "basic" ? "bg-indigo-600 text-white" : "bg-slate-800 text-slate-400"}`}
                                        >
                                          Mẫu cơ bản
                                        </button>
                                        <button
                                          onClick={() => setActiveExprTab("advanced")}
                                          className={`px-2 py-0.5 text-[10px] rounded ${activeExprTab === "advanced" ? "bg-indigo-600 text-white" : "bg-slate-800 text-slate-400"}`}
                                        >
                                          Cú pháp tự do
                                        </button>
                                      </div>
                                    </div>

                                    {activeExprTab === "basic" ? (
                                      <div className="space-y-2">
                                        <div className="grid grid-cols-3 gap-2">
                                          <select
                                            value={exprSelectedColumn}
                                            onChange={(e) => setExprSelectedColumn(e.target.value)}
                                            className="bg-[#070a13] border border-slate-800 rounded p-1.5 text-xs text-slate-300"
                                          >
                                            <option value="subinventoryCode">subinventoryCode</option>
                                            <option value="availableQuantity">availableQuantity</option>
                                            <option value="item">item</option>
                                            <option value="stopPo">stopPo</option>
                                          </select>

                                          <select
                                            value={exprOperator}
                                            onChange={(e) => setExprOperator(e.target.value)}
                                            className="bg-[#070a13] border border-slate-800 rounded p-1.5 text-xs text-slate-300"
                                          >
                                            <option value="==">bằng (==)</option>
                                            <option value="!=">khác (!=)</option>
                                            <option value=">">lớn hơn (&gt;)</option>
                                            <option value="<">nhỏ hơn (&lt;)</option>
                                            <option value="IN">thuộc tập hợp (IN)</option>
                                          </select>

                                          <input
                                            type="text"
                                            value={exprValue}
                                            onChange={(e) => setExprValue(e.target.value)}
                                            className="bg-[#070a13] border border-slate-800 rounded p-1.5 text-xs font-mono text-indigo-400"
                                            placeholder="['Value1', 'Value2']"
                                          />
                                        </div>
                                        <button
                                          type="button"
                                          onClick={applyExpressionToActiveOperator}
                                          className="w-full bg-indigo-600/20 hover:bg-indigo-600 border border-indigo-600 text-indigo-200 text-xs py-1 px-3 rounded transition"
                                        >
                                          Áp dụng Biểu thức
                                        </button>
                                      </div>
                                    ) : (
                                      <div className="space-y-2">
                                        <textarea
                                          value={exprValue}
                                          onChange={(e) => setExprValue(e.target.value)}
                                          rows="2"
                                          placeholder="Cú pháp EBNF ví dụ: availableQuantity < safetyStockQty"
                                          className="w-full bg-[#070a13] border border-slate-800 rounded p-2 text-xs font-mono text-emerald-400"
                                        />
                                        <button
                                          type="button"
                                          onClick={applyExpressionToActiveOperator}
                                          className="w-full bg-indigo-600/20 hover:bg-indigo-600 border border-indigo-600 text-indigo-200 text-xs py-1 px-3 rounded transition"
                                        >
                                          Áp dụng Biểu thức Tự do
                                        </button>
                                      </div>
                                    )}
                                  </div>

                                  <div>
                                    <label className="text-[10px] text-slate-400 block font-semibold">Biểu thức lọc hoàn chỉnh (condition)</label>
                                    <input
                                      type="text"
                                      value={activeOp.config?.condition || ""}
                                      onChange={(e) => updatePipelineOperatorDetail("config.condition", e.target.value)}
                                      className="w-full bg-[#070a13] border border-slate-800 rounded px-2.5 py-1.5 text-xs text-slate-300 font-mono mt-1"
                                    />
                                  </div>
                                </div>
                              )}

                              {/* Operator: TRANSFORM */}
                              {activeOp.operator === "TRANSFORM" && (
                                <div className="space-y-4">
                                  <div>
                                    <label className="text-[10px] text-slate-400 block font-semibold">Dataset Nguồn (input_dataset)</label>
                                    <input
                                      type="text"
                                      value={activeOp.inputs?.input_dataset || ""}
                                      onChange={(e) => updatePipelineOperatorDetail("inputs.input_dataset", e.target.value)}
                                      className="w-full bg-[#070a13] border border-slate-800 rounded px-2.5 py-1.5 text-xs text-indigo-400 font-mono mt-1"
                                    />
                                  </div>

                                  <div>
                                    <div className="flex items-center justify-between mb-1.5">
                                      <label className="text-[10px] text-slate-400 font-semibold uppercase">Danh sách biến đổi dữ liệu cột (operations)</label>
                                      <button
                                        type="button"
                                        onClick={() => {
                                          const currentOps = { ...(activeOp.config?.operations || {}) };
                                          currentOps[`new_col_${Object.keys(currentOps).length + 1}`] = "$.some_field";
                                          updatePipelineOperatorDetail("config.operations", currentOps);
                                        }}
                                        className="text-[10px] text-indigo-400 hover:underline"
                                      >
                                        + Thêm cột mới
                                      </button>
                                    </div>

                                    <div className="space-y-2 max-h-48 overflow-y-auto pr-1">
                                      {Object.entries(activeOp.config?.operations || {}).map(([col, expr], cIdx) => (
                                        <div key={cIdx} className="flex items-center space-x-2">
                                          <input
                                            type="text"
                                            value={col}
                                            onChange={(e) => {
                                              const newKey = e.target.value;
                                              const currentOps = { ...activeOp.config?.operations };
                                              delete currentOps[col];
                                              currentOps[newKey] = expr;
                                              updatePipelineOperatorDetail("config.operations", currentOps);
                                            }}
                                            className="w-1/3 bg-[#070a13] border border-slate-800 rounded px-2 py-1 text-xs font-mono text-amber-400"
                                            placeholder="Tên cột mới"
                                          />
                                          <input
                                            type="text"
                                            value={expr}
                                            onChange={(e) => {
                                              const currentOps = { ...activeOp.config?.operations };
                                              currentOps[col] = e.target.value;
                                              updatePipelineOperatorDetail("config.operations", currentOps);
                                            }}
                                            className="w-2/3 bg-[#070a13] border border-slate-800 rounded px-2 py-1 text-xs font-mono text-slate-300"
                                            placeholder="Biểu thức AST"
                                          />
                                          <button
                                            type="button"
                                            onClick={() => {
                                              const currentOps = { ...activeOp.config?.operations };
                                              delete currentOps[col];
                                              updatePipelineOperatorDetail("config.operations", currentOps);
                                            }}
                                            className="p-1 text-rose-500 hover:bg-slate-800 rounded"
                                          >
                                            ✕
                                          </button>
                                        </div>
                                      ))}
                                    </div>
                                  </div>
                                </div>
                              )}

                              {/* Operator: AGGREGATE */}
                              {activeOp.operator === "AGGREGATE" && (
                                <div className="space-y-4">
                                  <div>
                                    <label className="text-[10px] text-slate-400 block font-semibold">Dataset Nguồn (input_dataset)</label>
                                    <input
                                      type="text"
                                      value={activeOp.inputs?.input_dataset || ""}
                                      onChange={(e) => updatePipelineOperatorDetail("inputs.input_dataset", e.target.value)}
                                      className="w-full bg-[#070a13] border border-slate-800 rounded px-2.5 py-1.5 text-xs text-indigo-400 font-mono mt-1"
                                    />
                                  </div>

                                  <div className="grid grid-cols-2 gap-4">
                                    <div>
                                      <label className="text-[10px] text-slate-400 block font-semibold">Cột gom nhóm (group_by)</label>
                                      <input
                                        type="text"
                                        value={(activeOp.config?.group_by || []).join(', ')}
                                        onChange={(e) => {
                                          const arr = e.target.value.split(',').map(s => s.trim()).filter(Boolean);
                                          updatePipelineOperatorDetail("config.group_by", arr);
                                        }}
                                        className="w-full bg-[#070a13] border border-slate-800 rounded px-2 py-1 text-xs font-mono text-slate-300 mt-1"
                                        placeholder="ví dụ: partNo, subinventory"
                                      />
                                    </div>
                                    <div>
                                      <div className="flex items-center justify-between">
                                        <label className="text-[10px] text-slate-400 block font-semibold">Các phép tính (aggregations)</label>
                                        <button
                                          type="button"
                                          onClick={() => {
                                            const aggs = [...(activeOp.config?.aggregations || [])];
                                            aggs.push({ column: "availableQty", function: "SUM", alias: "sum_available" });
                                            updatePipelineOperatorDetail("config.aggregations", aggs);
                                          }}
                                          className="text-[10px] text-indigo-400 hover:underline"
                                        >
                                          + Thêm phép tính
                                        </button>
                                      </div>

                                      <div className="space-y-2 mt-1 max-h-36 overflow-y-auto">
                                        {(activeOp.config?.aggregations || []).map((agg, aIdx) => (
                                          <div key={aIdx} className="space-y-1 p-2 bg-slate-900 border border-slate-800 rounded relative">
                                            <button
                                              type="button"
                                              onClick={() => {
                                                const aggs = activeOp.config.aggregations.filter((_, i) => i !== aIdx);
                                                updatePipelineOperatorDetail("config.aggregations", aggs);
                                              }}
                                              className="absolute top-1 right-1 text-rose-500 text-xs"
                                            >
                                              ✕
                                            </button>
                                            <div className="grid grid-cols-3 gap-1">
                                              <input
                                                type="text"
                                                value={agg.column}
                                                onChange={(e) => {
                                                  const aggs = [...activeOp.config.aggregations];
                                                  aggs[aIdx].column = e.target.value;
                                                  updatePipelineOperatorDetail("config.aggregations", aggs);
                                                }}
                                                className="bg-[#070a13] border border-slate-800 rounded px-1 text-[10px] text-slate-300 font-mono"
                                                placeholder="Cột"
                                              />
                                              <select
                                                value={agg.function}
                                                onChange={(e) => {
                                                  const aggs = [...activeOp.config.aggregations];
                                                  aggs[aIdx].function = e.target.value;
                                                  updatePipelineOperatorDetail("config.aggregations", aggs);
                                                }}
                                                className="bg-[#070a13] border border-slate-800 rounded px-1 text-[10px] text-slate-300"
                                              >
                                                <option value="SUM">SUM</option>
                                                <option value="AVG">AVG</option>
                                                <option value="COUNT">COUNT</option>
                                                <option value="MAX">MAX</option>
                                                <option value="MIN">MIN</option>
                                              </select>
                                              <input
                                                type="text"
                                                value={agg.alias}
                                                onChange={(e) => {
                                                  const aggs = [...activeOp.config.aggregations];
                                                  aggs[aIdx].alias = e.target.value;
                                                  updatePipelineOperatorDetail("config.aggregations", aggs);
                                                }}
                                                className="bg-[#070a13] border border-slate-800 rounded px-1 text-[10px] text-slate-300 font-mono"
                                                placeholder="Bí danh"
                                              />
                                            </div>
                                          </div>
                                        ))}
                                      </div>
                                    </div>
                                  </div>
                                </div>
                              )}

                              {/* Operator: PIVOT */}
                              {activeOp.operator === "PIVOT" && (
                                <div className="space-y-3">
                                  <div>
                                    <label className="text-[10px] text-slate-400 block font-semibold">Dataset Nguồn (input_dataset)</label>
                                    <input
                                      type="text"
                                      value={activeOp.inputs?.input_dataset || ""}
                                      onChange={(e) => updatePipelineOperatorDetail("inputs.input_dataset", e.target.value)}
                                      className="w-full bg-[#070a13] border border-slate-800 rounded px-2.5 py-1.5 text-xs text-indigo-400 font-mono mt-1"
                                    />
                                  </div>
                                  <div className="grid grid-cols-3 gap-3">
                                    <div>
                                      <label className="text-[10px] text-slate-400 block font-semibold">Chỉ mục hàng (index)</label>
                                      <input
                                        type="text"
                                        value={(activeOp.config?.index || []).join(', ')}
                                        onChange={(e) => {
                                          const arr = e.target.value.split(',').map(s => s.trim()).filter(Boolean);
                                          updatePipelineOperatorDetail("config.index", arr);
                                        }}
                                        className="w-full bg-[#070a13] border border-slate-800 rounded px-2.5 py-1.5 text-xs font-mono text-slate-300 mt-1"
                                        placeholder="ví dụ: subinventory"
                                      />
                                    </div>
                                    <div>
                                      <label className="text-[10px] text-slate-400 block font-semibold">Cột PIVOT (pivot_column)</label>
                                      <input
                                        type="text"
                                        value={activeOp.config?.pivot_column || ""}
                                        onChange={(e) => updatePipelineOperatorDetail("config.pivot_column", e.target.value)}
                                        className="w-full bg-[#070a13] border border-slate-800 rounded px-2.5 py-1.5 text-xs font-mono text-slate-300 mt-1"
                                      />
                                    </div>
                                    <div>
                                      <label className="text-[10px] text-slate-400 block font-semibold">Cột giá trị (value_column)</label>
                                      <input
                                        type="text"
                                        value={activeOp.config?.value_column || ""}
                                        onChange={(e) => updatePipelineOperatorDetail("config.value_column", e.target.value)}
                                        className="w-full bg-[#070a13] border border-slate-800 rounded px-2.5 py-1.5 text-xs font-mono text-slate-300 mt-1"
                                      />
                                    </div>
                                  </div>
                                </div>
                              )}

                              {/* Operator: ACCUMULATE */}
                              {activeOp.operator === "ACCUMULATE" && (
                                <div className="space-y-3">
                                  <div>
                                    <label className="text-[10px] text-slate-400 block font-semibold">Dataset Nguồn (input_dataset)</label>
                                    <input
                                      type="text"
                                      value={activeOp.inputs?.input_dataset || ""}
                                      onChange={(e) => updatePipelineOperatorDetail("inputs.input_dataset", e.target.value)}
                                      className="w-full bg-[#070a13] border border-slate-800 rounded px-2.5 py-1.5 text-xs text-indigo-400 font-mono mt-1"
                                    />
                                  </div>
                                  <div className="grid grid-cols-2 gap-3">
                                    <div>
                                      <label className="text-[10px] text-slate-400 block font-semibold">Khóa lưu trữ Global Cache</label>
                                      <input
                                        type="text"
                                        value={activeOp.config?.accumulator_key || ""}
                                        onChange={(e) => updatePipelineOperatorDetail("config.accumulator_key", e.target.value)}
                                        className="w-full bg-[#070a13] border border-slate-800 rounded px-2.5 py-1.5 text-xs font-mono text-slate-300 mt-1"
                                      />
                                    </div>
                                    <div>
                                      <label className="text-[10px] text-slate-400 block font-semibold">Chiến lược gộp (merge_strategy)</label>
                                      <select
                                        value={activeOp.config?.merge_strategy || "UPSERT"}
                                        onChange={(e) => updatePipelineOperatorDetail("config.merge_strategy", e.target.value)}
                                        className="w-full bg-[#070a13] border border-slate-800 rounded px-2.5 py-1.5 text-xs text-slate-300 mt-1"
                                      >
                                        <option value="UPSERT">UPSERT</option>
                                        <option value="APPEND">APPEND ROWS</option>
                                        <option value="OVERWRITE">OVERWRITE ALL</option>
                                      </select>
                                    </div>
                                  </div>
                                </div>
                              )}
                            </div>
                          </div>
                        );
                      })()
                    ) : (
                      <div className="p-12 text-center text-slate-500 italic">
                        Hãy chọn một bước xử lý cụ thể từ cột bên trái để thiết lập cấu hình.
                      </div>
                    )}
                  </div>
                </div>
              )}
            </div>
          )}

          {/* TAB 3: SCHEMA REGISTRY MANAGER */}
          {activeTab === "schemas" && (
            <div className="flex-1 flex flex-col p-6 overflow-y-auto space-y-6">
              <div className="flex items-center justify-between pb-4 border-b border-slate-800">
                <div>
                  <h2 className="text-xl font-bold text-slate-200">Schema Registry Dashboard</h2>
                  <p className="text-xs text-slate-400">Đăng ký và tiền kiểm tra cấu trúc dữ liệu tĩnh để kích hoạt cơ chế Fail-Fast bảo vệ luồng chạy.</p>
                </div>
                <button
                  onClick={() => {
                    const nextSchemaName = `Schema_NewDataset_${Date.now().toString().slice(-3)}`;
                    handleSetWorkflow(prev => {
                      const updated = { ...prev.schema_registry };
                      updated[nextSchemaName] = {
                        type: "DATASET",
                        columns: [{ name: "id", type: "STRING", nullable: false }]
                      };
                      return { ...prev, schema_registry: updated };
                    });
                    setActiveSchemaTab(nextSchemaName);
                    showToast("Đã bổ sung Schema tĩnh mới!");
                  }}
                  className="px-3 py-1.5 bg-indigo-600 hover:bg-indigo-700 text-slate-200 text-xs font-semibold rounded shadow transition"
                >
                  + Đăng ký Schema mới
                </button>
              </div>

              <div className="grid grid-cols-12 gap-6">
                
                {/* Khung tabs chọn schema */}
                <div className="col-span-3 flex flex-col space-y-1">
                  {Object.keys(workflow.schema_registry || {}).map(schemaName => {
                    const isActive = activeSchemaTab === schemaName;
                    const schema = workflow.schema_registry[schemaName];
                    return (
                      <button
                        key={schemaName}
                        onClick={() => setActiveSchemaTab(schemaName)}
                        className={`px-3 py-2.5 rounded-lg text-left text-xs font-medium transition flex items-center justify-between ${
                          isActive ? "bg-indigo-950 text-indigo-300 border border-indigo-800" : "bg-[#111625] text-slate-400 border border-transparent hover:bg-slate-800/40"
                        }`}
                      >
                        <span className="font-mono">{schemaName}</span>
                        <span className="text-[9px] px-1 bg-black/50 rounded text-slate-500">{schema.type}</span>
                      </button>
                    );
                  })}
                </div>

                {/* Khung biên tập cột tĩnh */}
                <div className="col-span-9 bg-[#0b0f19] border border-slate-800 rounded-xl p-6">
                  {activeSchemaTab && workflow.schema_registry?.[activeSchemaTab] ? (
                    (() => {
                      const currentSchema = workflow.schema_registry[activeSchemaTab];
                      return (
                        <div className="space-y-4">
                          <div className="flex justify-between items-center pb-3 border-b border-slate-800">
                            <div>
                              <h3 className="text-sm font-bold text-slate-200">Chi tiết: {activeSchemaTab}</h3>
                              <p className="text-[10px] text-slate-400">Cấu hình các cột định dạng chặt chẽ</p>
                            </div>
                            {currentSchema.type === "DATASET" && (
                              <button
                                onClick={() => handleAddSchemaColumn(activeSchemaTab)}
                                className="px-2.5 py-1 bg-indigo-950 hover:bg-indigo-900 text-indigo-300 text-[11px] font-semibold border border-indigo-800 rounded"
                              >
                                + Thêm cột tĩnh
                              </button>
                            )}
                          </div>

                          {currentSchema.type === "OBJECT" ? (
                            <div className="space-y-4">
                              <span className="text-xs text-amber-400 block">ℹ️ Mô hình JSON Schema được cấu trúc phân cấp (Draft 2020-12) dùng định dạng form tương tác:</span>
                              <pre className="bg-[#111625] p-3 rounded-lg border border-slate-800 text-xs font-mono text-slate-300 overflow-x-auto max-h-96">
                                {JSON.stringify(currentSchema, null, 2)}
                              </pre>
                            </div>
                          ) : (
                            <div className="space-y-2">
                              <div className="grid grid-cols-12 gap-2 text-[10px] text-slate-500 font-bold uppercase pb-1 border-b border-slate-800/40">
                                <span className="col-span-4">Tên cột</span>
                                <span className="col-span-3">Kiểu dữ liệu</span>
                                <span className="col-span-2">Cho phép Null</span>
                                <span className="col-span-2">Giá trị mặc định</span>
                                <span className="col-span-1 text-right">Hành động</span>
                              </div>

                              <div className="space-y-2 max-h-96 overflow-y-auto pr-1">
                                {(currentSchema.columns || []).map((col, idx) => (
                                  <div key={idx} className="grid grid-cols-12 gap-2 items-center bg-[#111625] p-2 rounded border border-slate-850">
                                    <div className="col-span-4">
                                      <input
                                        type="text"
                                        value={col.name}
                                        onChange={(e) => handleUpdateSchemaColumn(activeSchemaTab, idx, "name", e.target.value)}
                                        className="w-full bg-[#070a13] border border-slate-800 rounded px-2 py-1 text-xs text-indigo-400 font-mono"
                                      />
                                    </div>
                                    <div className="col-span-3">
                                      <select
                                        value={col.type}
                                        onChange={(e) => handleUpdateSchemaColumn(activeSchemaTab, idx, "type", e.target.value)}
                                        className="w-full bg-[#070a13] border border-slate-800 rounded px-2 py-1 text-xs text-slate-300"
                                      >
                                        <option value="STRING">STRING</option>
                                        <option value="DECIMAL">DECIMAL</option>
                                        <option value="INTEGER">INTEGER</option>
                                        <option value="DATETIME">DATETIME</option>
                                        <option value="BOOLEAN">BOOLEAN</option>
                                      </select>
                                    </div>
                                    <div className="col-span-2 text-center">
                                      <input
                                        type="checkbox"
                                        checked={col.nullable !== false}
                                        onChange={(e) => handleUpdateSchemaColumn(activeSchemaTab, idx, "nullable", e.target.checked)}
                                        className="rounded border-slate-800 text-indigo-600 bg-[#070a13]"
                                      />
                                    </div>
                                    <div className="col-span-2">
                                      <input
                                        type="text"
                                        value={col.default !== undefined ? col.default : ""}
                                        onChange={(e) => handleUpdateSchemaColumn(activeSchemaTab, idx, "default", e.target.value)}
                                        className="w-full bg-[#070a13] border border-slate-800 rounded px-2 py-1 text-xs text-slate-400 font-mono"
                                        placeholder="None"
                                      />
                                    </div>
                                    <div className="col-span-1 text-right">
                                      <button
                                        type="button"
                                        onClick={() => handleRemoveSchemaColumn(activeSchemaTab, idx)}
                                        className="p-1 text-rose-500 hover:bg-slate-850 rounded"
                                      >
                                        ✕
                                      </button>
                                    </div>
                                  </div>
                                ))}
                              </div>
                            </div>
                          )}
                        </div>
                      );
                    })()
                  ) : (
                    <div className="text-center text-slate-500 italic py-12">
                      Chọn một Schema để bắt đầu chỉnh sửa dữ liệu tĩnh.
                    </div>
                  )}
                </div>
              </div>
            </div>
          )}

          {/* TAB 4: GLOBAL VARIABLES & SECRET CONTEXT */}
          {activeTab === "context" && (
            <div className="flex-1 p-6 overflow-y-auto space-y-6">
              <div className="pb-4 border-b border-slate-800">
                <h2 className="text-xl font-bold text-slate-200">Global Variable & Secrets Context</h2>
                <p className="text-xs text-slate-400">Môi trường phân lập bảo mật cao, cô lập biến giữa các Tenant.</p>
              </div>

              <div className="grid grid-cols-2 gap-6">
                
                {/* Global Variables */}
                <div className="bg-[#0b0f19] border border-slate-800 rounded-xl p-5 space-y-4">
                  <div className="flex justify-between items-center border-b border-slate-800 pb-2">
                    <span className="text-xs font-bold text-slate-300 uppercase tracking-wider">Biến toàn cục (global.variables)</span>
                    <button
                      onClick={() => {
                        handleSetWorkflow(prev => {
                          const ctx = { ...prev.context };
                          ctx.scopes.global.variables = {
                            ...ctx.scopes.global.variables,
                            [`new_var_${Date.now().toString().slice(-3)}`]: "mặc định"
                          };
                          return { ...prev, context: ctx };
                        });
                      }}
                      className="text-[10px] text-indigo-400 hover:underline"
                    >
                      + Thêm biến
                    </button>
                  </div>

                  <div className="space-y-2">
                    {Object.entries(workflow.context?.scopes?.global?.variables || {}).map(([key, val]) => (
                      <div key={key} className="flex items-center space-x-2">
                        <input
                          type="text"
                          value={key}
                          onChange={(e) => {
                            const newKey = e.target.value;
                            handleSetWorkflow(prev => {
                              const variables = { ...prev.context.scopes.global.variables };
                              delete variables[key];
                              variables[newKey] = val;
                              return {
                                ...prev,
                                context: {
                                  ...prev.context,
                                  scopes: {
                                    ...prev.context.scopes,
                                    global: { ...prev.context.scopes.global, variables }
                                  }
                                }
                              };
                            });
                          }}
                          className="w-1/3 bg-[#111625] border border-slate-800 rounded px-2 py-1 text-xs font-mono text-indigo-300"
                        />
                        <input
                          type="text"
                          value={val}
                          onChange={(e) => {
                            const newVal = e.target.value;
                            handleSetWorkflow(prev => {
                              const variables = { ...prev.context.scopes.global.variables };
                              variables[key] = newVal;
                              return {
                                ...prev,
                                context: {
                                  ...prev.context,
                                  scopes: {
                                    ...prev.context.scopes,
                                    global: { ...prev.context.scopes.global, variables }
                                  }
                                }
                              };
                            });
                          }}
                          className="w-2/3 bg-[#111625] border border-slate-800 rounded px-2 py-1 text-xs font-mono text-slate-200"
                        />
                      </div>
                    ))}
                  </div>
                </div>

                {/* Secrets (Vault Mappings) */}
                <div className="bg-[#0b0f19] border border-slate-800 rounded-xl p-5 space-y-4">
                  <div className="flex justify-between items-center border-b border-slate-800 pb-2">
                    <span className="text-xs font-bold text-slate-300 uppercase tracking-wider">Hòm khóa bảo mật (global.secrets)</span>
                    <span className="text-[10px] text-rose-400 font-mono">Vault mã hóa bảo vệ chống lộ API key</span>
                  </div>

                  <div className="space-y-2">
                    {Object.entries(workflow.context?.scopes?.global?.secrets || {}).map(([key, val]) => (
                      <div key={key} className="flex items-center space-x-2">
                        <span className="w-1/3 text-xs font-mono text-slate-400">{key}</span>
                        <input
                          type="text"
                          value={val}
                          onChange={(e) => {
                            const newVal = e.target.value;
                            handleSetWorkflow(prev => {
                              const secrets = { ...prev.context.scopes.global.secrets };
                              secrets[key] = newVal;
                              return {
                                ...prev,
                                context: {
                                  ...prev.context,
                                  scopes: {
                                    ...prev.context.scopes,
                                    global: { ...prev.context.scopes.global, secrets }
                                  }
                                }
                              };
                            });
                          }}
                          className="w-2/3 bg-[#111625] border border-slate-800 rounded px-2 py-1 text-xs font-mono text-slate-300"
                        />
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            </div>
          )}
        </main>

        {/* ==========================================
            PROPERTY PANEL (CẤU HÌNH THÔNG SỐ NODE ĐANG CHỌN)
            ========================================== */}
        <aside className="w-80 bg-[#0b0f19] border-l border-slate-800 overflow-y-auto flex flex-col p-4 shrink-0 space-y-6">
          <div className="flex justify-between items-center border-b border-slate-800 pb-3">
            <span className="text-xs font-bold text-slate-400 uppercase tracking-widest">Property Panel</span>
            <span className="px-1.5 py-0.5 text-[9px] bg-indigo-950 text-indigo-300 rounded font-semibold">Active Node</span>
          </div>

          {selectedNodeId && workflow.nodes?.[selectedNodeId] ? (
            (() => {
              const node = workflow.nodes[selectedNodeId];
              return (
                <div className="space-y-6">
                  
                  {/* Meta Node */}
                  <div className="space-y-3">
                    <div>
                      <label className="block text-[10px] text-slate-500 font-semibold uppercase">Tên hiển thị (Label)</label>
                      <input
                        type="text"
                        value={node.label || ""}
                        onChange={(e) => updateNodeMeta("label", e.target.value)}
                        className="w-full bg-[#111625] border border-slate-800 rounded px-2.5 py-1.5 text-xs text-slate-200 mt-1"
                      />
                    </div>
                    <div>
                      <label className="block text-[10px] text-slate-500 font-semibold uppercase">Mối liên kết phụ thuộc (depends_on)</label>
                      <div className="mt-1 space-y-1">
                        {Object.keys(workflow.nodes)
                          .filter(k => k !== selectedNodeId)
                          .map(k => {
                            const isLinked = (node.depends_on || []).includes(k);
                            return (
                              <label key={k} className="flex items-center space-x-2 text-xs text-slate-300 p-1.5 bg-[#111625] rounded cursor-pointer border border-slate-850 hover:bg-slate-800/40">
                                <input
                                  type="checkbox"
                                  checked={isLinked}
                                  onChange={() => handleToggleDependency(selectedNodeId, k)}
                                  className="rounded text-indigo-600 bg-slate-900 border-slate-800"
                                />
                                <span className="font-mono">{k}</span>
                              </label>
                            );
                          })}
                      </div>
                    </div>
                  </div>

                  {/* Cấu hình đặc trưng theo loại NODE */}
                  <div className="border-t border-slate-800 pt-4 space-y-4">
                    <span className="text-xs font-bold text-slate-300 block uppercase tracking-wide">Cấu hình Động</span>

                    {/* NODE: USER_INTERACTION */}
                    {node.type === "USER_INTERACTION" && (
                      <div className="space-y-4">
                        <div>
                          <label className="block text-[10px] text-slate-400 font-semibold">Output Schema liên quan</label>
                          <select
                            value={node.config?.output_schema || ""}
                            onChange={(e) => updateNodeConfig("output_schema", e.target.value)}
                            className="w-full bg-[#111625] border border-slate-800 rounded px-2 py-1.5 text-xs text-indigo-400 font-mono mt-1"
                          >
                            <option value="">-- Chọn Schema Registry --</option>
                            {Object.keys(workflow.schema_registry || {}).map(s => (
                              <option key={s} value={s}>{s}</option>
                            ))}
                          </select>
                        </div>

                        {/* Thêm trường Form Builder trực quan */}
                        <div className="space-y-3">
                          <div className="flex justify-between items-center">
                            <span className="text-[10px] text-slate-400 font-semibold uppercase">Cấu trúc Form Trường</span>
                            <button
                              type="button"
                              onClick={handleAddArrayField}
                              className="text-[10px] text-indigo-400 hover:underline"
                            >
                              + Thêm trường
                            </button>
                          </div>

                          <div className="space-y-2 max-h-56 overflow-y-auto">
                            {(node.config?.fields || []).map((field, fIdx) => (
                              <div key={fIdx} className="p-2.5 bg-[#111625] rounded border border-slate-800 space-y-2">
                                <div className="flex justify-between items-center">
                                  <span className="text-[10px] font-mono text-slate-400">Trường #{fIdx + 1}</span>
                                  <button
                                    type="button"
                                    onClick={() => handleRemoveArrayField(fIdx)}
                                    className="text-[9px] text-rose-500"
                                  >
                                    Xóa
                                  </button>
                                </div>
                                <div className="grid grid-cols-2 gap-2">
                                  <input
                                    type="text"
                                    value={field.name || ""}
                                    placeholder="Tên trường"
                                    onChange={(e) => {
                                      const fields = [...node.config.fields];
                                      fields[fIdx].name = e.target.value;
                                      updateNodeConfig("fields", fields);
                                    }}
                                    className="bg-[#070a13] border border-slate-800 rounded px-2 py-1 text-xs text-slate-300"
                                  />
                                  <select
                                    value={field.type || "STRING"}
                                    onChange={(e) => {
                                      const fields = [...node.config.fields];
                                      fields[fIdx].type = e.target.value;
                                      updateNodeConfig("fields", fields);
                                    }}
                                    className="bg-[#070a13] border border-slate-800 rounded px-2 py-1 text-xs text-slate-300"
                                  >
                                    <option value="STRING">TEXT</option>
                                    <option value="TEXT_AREA">TEXT_AREA</option>
                                    <option value="SELECT">SELECT</option>
                                  </select>
                                </div>
                              </div>
                            ))}
                          </div>
                        </div>
                      </div>
                    )}

                    {/* NODE: CONNECTOR_HTTP */}
                    {node.type === "CONNECTOR_HTTP" && (
                      <div className="space-y-4">
                        <div>
                          <label className="block text-[10px] text-slate-400 font-semibold">Endpoint API URL</label>
                          <input
                            type="text"
                            value={node.config?.connection?.url || ""}
                            onChange={(e) => updateNodeConfig("connection.url", e.target.value)}
                            className="w-full bg-[#111625] border border-slate-800 rounded px-2.5 py-1.5 text-xs text-indigo-400 font-mono mt-1"
                            placeholder="http://example.com/api"
                          />
                        </div>
                        <div className="grid grid-cols-2 gap-3">
                          <div>
                            <label className="block text-[10px] text-slate-400 font-semibold">HTTP Method</label>
                            <select
                              value={node.config?.connection?.method || "POST"}
                              onChange={(e) => updateNodeConfig("connection.method", e.target.value)}
                              className="w-full bg-[#111625] border border-slate-800 rounded px-2 py-1.5 text-xs text-slate-200 mt-1"
                            >
                              <option value="GET">GET</option>
                              <option value="POST">POST</option>
                              <option value="PUT">PUT</option>
                              <option value="DELETE">DELETE</option>
                            </select>
                          </div>
                          <div>
                            <label className="block text-[10px] text-slate-400 font-semibold">Timeout (ms)</label>
                            <input
                              type="number"
                              value={node.config?.connection?.timeout_ms || 3000}
                              onChange={(e) => updateNodeConfig("connection.timeout_ms", parseInt(e.target.value))}
                              className="w-full bg-[#111625] border border-slate-800 rounded px-2 py-1 text-xs text-slate-300 font-mono mt-1"
                            />
                          </div>
                        </div>

                        {/* Vòng lặp HTTP Loop Over */}
                        <div className="p-3 bg-indigo-950/20 rounded-lg border border-indigo-900/40 space-y-3">
                          <span className="text-[10px] font-bold text-indigo-300 uppercase block">Thực thi Lặp Concurrent</span>
                          <div>
                            <label className="block text-[9px] text-slate-400 font-semibold">Biểu thức lặp (loop_over)</label>
                            <input
                              type="text"
                              value={node.config?.execution?.loop_over || ""}
                              onChange={(e) => updateNodeConfig("execution.loop_over", e.target.value)}
                              className="w-full bg-[#111625] border border-slate-800 rounded px-2 py-1 text-xs text-slate-300 font-mono mt-1"
                              placeholder="$.steps.node_user_input.output"
                            />
                          </div>
                          <div className="grid grid-cols-2 gap-2">
                            <div>
                              <label className="block text-[9px] text-slate-400 font-semibold">Luồng tối đa</label>
                              <input
                                type="number"
                                value={node.config?.execution?.max_concurrency || 5}
                                onChange={(e) => updateNodeConfig("execution.max_concurrency", parseInt(e.target.value))}
                                className="w-full bg-[#111625] border border-slate-800 rounded px-2 py-1 text-xs text-slate-300 font-mono mt-1"
                              />
                            </div>
                            <div>
                              <label className="block text-[9px] text-slate-400 font-semibold">Gộp kết quả</label>
                              <select
                                value={node.config?.execution?.aggregation_strategy || "UNION_ALL"}
                                onChange={(e) => updateNodeConfig("execution.aggregation_strategy", e.target.value)}
                                className="w-full bg-[#111625] border border-slate-800 rounded px-2 py-1 text-xs text-slate-300 mt-1"
                              >
                                <option value="UNION_ALL">UNION_ALL</option>
                                <option value="CONCAT_ROWS">CONCAT_ROWS</option>
                              </select>
                            </div>
                          </div>
                        </div>
                      </div>
                    )}

                    {/* NODE: USER_DISPLAY */}
                    {node.type === "USER_DISPLAY" && (
                      <div className="space-y-4">
                        <div>
                          <label className="block text-[10px] text-slate-400 font-semibold">Dataset Nguồn Đầu Vào</label>
                          <input
                            type="text"
                            value={node.config?.input_dataset || ""}
                            onChange={(e) => updateNodeConfig("input_dataset", e.target.value)}
                            className="w-full bg-[#111625] border border-slate-800 rounded px-2.5 py-1.5 text-xs text-indigo-400 font-mono mt-1"
                          />
                        </div>
                        <div>
                          <label className="block text-[10px] text-slate-400 font-semibold">UI Component</label>
                          <select
                            value={node.config?.ui_component || "DATA_GRID"}
                            onChange={(e) => updateNodeConfig("ui_component", e.target.value)}
                            className="w-full bg-[#111625] border border-slate-800 rounded px-2.5 py-1.5 text-xs text-slate-300 mt-1"
                          >
                            <option value="DATA_GRID">DATA_GRID (Lưới bảng biểu)</option>
                            <option value="CHART_BAR">CHART_BAR (Biểu đồ cột)</option>
                            <option value="JSON_VIEW">JSON_VIEW (Khung text thô)</option>
                          </select>
                        </div>
                      </div>
                    )}

                    {/* ERROR POLICY (Chính sách lỗi riêng cấp Node) */}
                    <div className="p-3 bg-slate-900 rounded-lg border border-slate-800 space-y-3">
                      <span className="text-[10px] font-bold text-slate-400 uppercase block">Chính sách xử lý lỗi</span>
                      <div className="grid grid-cols-2 gap-2">
                        <div>
                          <label className="text-[9px] text-slate-500 font-semibold block">Số lần Thử lại</label>
                          <input
                            type="number"
                            value={node.error_policy?.retry?.max_attempts || 0}
                            onChange={(e) => updateNodeConfig("error_policy.retry.max_attempts", parseInt(e.target.value))}
                            className="w-full bg-[#111625] border border-slate-800 rounded px-2 py-1 text-xs font-mono text-slate-300 mt-1"
                          />
                        </div>
                        <div>
                          <label className="text-[9px] text-slate-500 font-semibold block">Hành động lỗi</label>
                          <select
                            value={node.error_policy?.on_failure || "FAIL_FAST"}
                            onChange={(e) => updateNodeConfig("error_policy.on_failure", e.target.value)}
                            className="w-full bg-[#111625] border border-slate-800 rounded px-2 py-1 text-xs text-slate-300 mt-1"
                          >
                            <option value="FAIL_FAST">FAIL_FAST (Dừng ngay)</option>
                            <option value="CONTINUE">CONTINUE (Bỏ qua tiếp tục)</option>
                          </select>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              );
            })()
          ) : (
            <div className="text-center text-slate-500 italic py-12">
              Hãy bấm chọn một Node trên Canvas hoặc Pipeline để cấu hình chi tiết thông số.
            </div>
          )}
        </aside>
      </div>

      {/* ==========================================
          BOTTOM DRAWER: GENERATED CONFIG PREVIEW & EXPORT/IMPORT
          ========================================== */}
      <footer className="bg-[#0b0f19] border-t border-slate-800 h-64 flex flex-col shrink-0 z-40">
        <div className="bg-[#090c14] border-b border-slate-850 px-6 py-2.5 flex items-center justify-between">
          <div className="flex items-center space-x-3">
            <span className="text-xs font-bold text-slate-300 uppercase tracking-widest">
              Generated JSON Configuration Specification (9.0.0)
            </span>
            <span className="flex h-2 w-2 relative">
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-violet-400 opacity-75"></span>
              <span className="relative inline-flex rounded-full h-2 w-2 bg-violet-500"></span>
            </span>
          </div>

          <div className="flex items-center space-x-2">
            <button
              onClick={() => setJsonMinified(!jsonMinified)}
              className="px-2.5 py-1 text-[11px] bg-slate-800 hover:bg-slate-700 text-slate-300 rounded border border-slate-700 font-semibold"
            >
              {jsonMinified ? "Pretty Format" : "Minify"}
            </button>
            <button
              onClick={() => copyToClipboard(JSON.stringify(workflow, null, jsonMinified ? 0 : 2))}
              className="px-2.5 py-1 text-[11px] bg-indigo-600 hover:bg-indigo-700 text-slate-100 rounded font-semibold transition"
            >
              Copy To Clipboard
            </button>
            <button
              onClick={() => {
                const dataStr = "data:text/json;charset=utf-8," + encodeURIComponent(JSON.stringify(workflow, null, 2));
                const downloadAnchor = document.createElement('a');
                downloadAnchor.setAttribute("href", dataStr);
                downloadAnchor.setAttribute("download", `${workflow.workflow_id}_v9.json`);
                document.body.appendChild(downloadAnchor);
                downloadAnchor.click();
                downloadAnchor.remove();
                showToast("Đã xuất và tải tệp JSON thành công!");
              }}
              className="px-2.5 py-1 text-[11px] bg-slate-800 hover:bg-slate-750 text-slate-200 rounded border border-slate-700 font-semibold"
            >
              Tải cấu hình (.json)
            </button>
          </div>
        </div>

        {/* Khung Text hiển thị JSON trực quan, đồng bộ hóa thời gian thực (Cấm người dùng sửa thủ công) */}
        <div className="flex-1 p-4 bg-[#07090f] overflow-auto select-text">
          <pre className="text-xs font-mono text-emerald-400 leading-relaxed whitespace-pre">
            {jsonMinified ? JSON.stringify(workflow) : JSON.stringify(workflow, null, 2)}
          </pre>
        </div>
      </footer>
    </div>
  );
}
