import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import requests
import xml.etree.ElementTree as ET
import threading
import re
import csv
import queue

# Kiểm tra thư viện pandas để hỗ trợ xuất Excel trực tiếp
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

# --- DỮ LIỆU GIẢ LẬP ĐỂ CHẠY DEMO OFFLINE ---
MOCK_XML_STEP1 = """<Root xmlns="http://www.nexacroplatform.com/platform/dataset">
<Dataset id="ds_List">
<ColumnInfo>
<Column id="uniqueNo" type="string" size="32"/>
<Column id="itemType" type="string" size="32"/>
<Column id="item" type="string" size="32"/>
<Column id="itemDescription" type="string" size="32"/>
<Column id="primaryUomCode" type="string" size="32"/>
<Column id="itemCost" type="string" size="32"/>
<Column id="buyerName" type="string" size="32"/>
<Column id="itemStatus" type="string" size="32"/>
<Column id="inventoryItemId" type="string" size="32"/>
<Column id="organizationId" type="string" size="32"/>
<Column id="poType" type="string" size="32"/>
</ColumnInfo>
<Rows>
<Row>
<Col id="uniqueNo">325</Col>
<Col id="itemType">SP</Col>
<Col id="item">QCAG26581</Col>
<Col id="itemDescription">Optic Accessory ** H1592190, TRUMPF, Application:POLARIZATION CONTROL</Col>
<Col id="primaryUomCode">EA</Col>
<Col id="itemCost">34305.1</Col>
<Col id="buyerName">Thi Ngoc Mai, Nguyen</Col>
<Col id="itemStatus">Active</Col>
<Col id="inventoryItemId">2232682</Col>
<Col id="organizationId">28753</Col>
<Col id="poType">Purchase of original corporation</Col>
</Row>
<Row>
<Col id="uniqueNo">326</Col>
<Col id="itemType">SP</Col>
<Col id="item">QCAD17495</Col>
<Col id="itemDescription">Sensor Cable ** 5m, Omron, Application:PHOTOELECTRIC SENSOR</Col>
<Col id="primaryUomCode">EA</Col>
<Col id="itemCost">125.5</Col>
<Col id="buyerName">Thi Ngoc Mai, Nguyen</Col>
<Col id="itemStatus">Active</Col>
<Col id="inventoryItemId">2245901</Col>
<Col id="organizationId">28753</Col>
<Col id="poType">Purchase of original corporation</Col>
</Row>
</Rows>
</Dataset>
</Root>"""

MOCK_XML_STEP2 = """<Root xmlns="http://www.nexacroplatform.com/platform/dataset">
<Dataset id="ds_ohlist">
<ColumnInfo>
<Column id="lpnId" type="string" size="32"/>
<Column id="partNo" type="string" size="32"/>
<Column id="makerModel" type="string" size="32"/>
<Column id="maker" type="string" size="32"/>
<Column id="subinventoryCode" type="string" size="32"/>
<Column id="grade" type="string" size="32"/>
<Column id="qty" type="string" size="32"/>
<Column id="unitPriceNew" type="string" size="32"/>
<Column id="receiptDate" type="string" size="32"/>
<Column id="requestTeam" type="string" size="32"/>
<Column id="requestor" type="string" size="32"/>
<Column id="onhandAmount" type="string" size="32"/>
</ColumnInfo>
<Rows>
<Row>
<Col id="lpnId">407545594</Col>
<Col id="partNo">QCAG26581</Col>
<Col id="makerModel">H1592190</Col>
<Col id="maker">TRUMPF</Col>
<Col id="subinventoryCode">SP-AA-UNL</Col>
<Col id="grade">A</Col>
<Col id="qty">1</Col>
<Col id="unitPriceNew">34305.1</Col>
<Col id="receiptDate">20260212000000000</Col>
<Col id="requestTeam">Auto Technology DB Part</Col>
<Col id="requestor">Pham Thi Thao</Col>
<Col id="onhandAmount">34305.1</Col>
</Row>
<Row>
<Col id="lpnId">411889986</Col>
<Col id="partNo">QCAG26581</Col>
<Col id="makerModel">H1592190</Col>
<Col id="maker">TRUMPF</Col>
<Col id="subinventoryCode">SP-C-NEW</Col>
<Col id="grade">Z</Col>
<Col id="qty">1</Col>
<Col id="unitPriceNew">34305.1</Col>
<Col id="receiptDate">20260528000000000</Col>
<Col id="requestTeam">NY SW PO M Technology Lami/Laser</Col>
<Col id="requestor">Cao Hong Luong</Col>
<Col id="onhandAmount">34305.1</Col>
</Row>
<Row>
<Col id="lpnId">412998877</Col>
<Col id="partNo">QCAD17495</Col>
<Col id="makerModel">5m Cable</Col>
<Col id="maker">OMRON</Col>
<Col id="subinventoryCode">SP-A-NEW</Col>
<Col id="grade">A</Col>
<Col id="qty">15</Col>
<Col id="unitPriceNew">125.5</Col>
<Col id="receiptDate">20260401000000000</Col>
<Col id="requestTeam">Auto Technology Sensor Part</Col>
<Col id="requestor">Nguyen Van A</Col>
<Col id="onhandAmount">1882.5</Col>
</Row>
</Rows>
</Dataset>
</Root>"""

# --- CẤU TRÚC LOGIC XML GENERATOR ---
def build_xml_step1(item, item_type, org_id, enabled_flag):
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<Root xmlns="http://www.nexacroplatform.com/platform/dataset">
  <Dataset id="ds_Search">
    <ColumnInfo>
      <Column id="item" type="STRING" size="256" />
      <Column id="itemType" type="STRING" size="256" />
      <Column id="organizationId" type="STRING" size="256" />
      <Column id="enabledFlag" type="STRING" size="256" />
    </ColumnInfo>
    <Rows>
      <Row>
        <Col id="item">{item}</Col>
        <Col id="itemType">{item_type}</Col>
        <Col id="organizationId">{org_id}</Col>
        <Col id="enabledFlag">{enabled_flag}</Col>
      </Row>
    </Rows>
  </Dataset>
</Root>"""

def build_xml_step2(org_id, user_id, inventory_item_id):
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<Root xmlns="http://www.nexacroplatform.com/platform/dataset">
  <Dataset id="ds_info">
    <ColumnInfo>
      <Column id="organizationId" type="STRING" size="256" />
      <Column id="userId" type="STRING" size="256" />
      <Column id="command" type="STRING" size="256" />
      <Column id="inventoryItemId" type="STRING" size="256" />
    </ColumnInfo>
    <Rows>
      <Row>
        <Col id="organizationId">{org_id}</Col>
        <Col id="userId">{user_id}</Col>
        <Col id="command">ItemSelect</Col>
        <Col id="inventoryItemId">{inventory_item_id}</Col>
      </Row>
    </Rows>
  </Dataset>
</Root>"""

# --- PHÂN TÍCH XML NEXACRO ---
def parse_nexacro_xml(xml_content, dataset_id):
    try:
        root = ET.fromstring(xml_content)
        # Khai báo Namespace Nexacro
        ns = {'ns': 'http://www.nexacroplatform.com/platform/dataset'}
        
        # Tìm Dataset theo ID
        target_dataset = None
        for ds in root.findall('ns:Dataset', ns):
            if ds.attrib.get('id') == dataset_id:
                target_dataset = ds
                break
                
        if target_dataset is None:
            return []
            
        records = []
        for row in target_dataset.findall('.//ns:Row', ns):
            row_data = {}
            for col in row.findall('ns:Col', ns):
                col_id = col.attrib.get('id')
                col_value = col.text if col.text is not None else ""
                row_data[col_id] = col_value
            records.append(row_data)
        return records
    except Exception as e:
        print(f"Lỗi khi Parse XML: {e}")
        return []


# --- GIAO DIỆN CHÍNH (GUI) ---
class EMSToolApp:
    def __init__(self, root):
        self.root = root
        self.root.title("LG Display - EMS XML Automator (Quy trình 2 bước)")
        self.root.geometry("1200x800")
        
        # Biến lưu trữ dữ liệu
        self.step1_raw_data = []
        self.step2_raw_data = []
        self.gui_queue = queue.Queue()
        
        # Thiết lập màu sắc & Phong cách Dark Theme chuyên nghiệp
        self.style = ttk.Style()
        self.style.theme_use('clam')
        self.setup_theme()
        
        # Vẽ giao diện
        self.build_ui()
        
        # Khởi chạy bộ lắng nghe sự kiện luồng (Queue listener)
        self.root.after(100, self.process_queue)

    def setup_theme(self):
        # Slate Dark Palette
        bg_dark = "#1e293b"
        bg_darker = "#0f172a"
        fg_white = "#f8fafc"
        accent_blue = "#6366f1"
        
        self.root.configure(bg=bg_darker)
        self.style.configure(".", background=bg_dark, foreground=fg_white, fieldbackground=bg_darker)
        self.style.configure("TFrame", background=bg_darker)
        self.style.configure("TLabel", background=bg_dark, foreground=fg_white, font=("Segoe UI", 10))
        self.style.configure("Header.TLabel", background=bg_darker, font=("Segoe UI", 14, "bold"), foreground="#818cf8")
        
        # Custom Cấu hình Buttons
        self.style.configure("TButton", background=accent_blue, foreground="white", font=("Segoe UI", 10, "bold"), borderwidth=0)
        self.style.map("TButton", background=[("active", "#4f46e5"), ("disabled", "#475569")])
        self.style.configure("Accent.TButton", background="#10b981", foreground="white") # Emerald Green
        self.style.map("Accent.TButton", background=[("active", "#059669")])
        
        # Cấu hình Entry & Text
        self.style.configure("TEntry", fieldbackground="#0f172a", foreground="white")
        
        # Cấu hình Tabs Notebook
        self.style.configure("TNotebook", background=bg_darker, borderwidth=0)
        self.style.configure("TNotebook.Tab", background=bg_dark, foreground="#94a3b8", font=("Segoe UI", 9, "bold"), padding=[12, 6])
        self.style.map("TNotebook.Tab", background=[("selected", "#6366f1")], foreground=[("selected", "white")])

    def build_ui(self):
        # 1. Khung Tiêu Đề Trên Cùng
        header_frame = ttk.Frame(self.root)
        header_frame.pack(fill=tk.X, padx=15, pady=10)
        
        title_lbl = ttk.Label(header_frame, text="LG DISPLAY - EMS TRACKING SYSTEM", style="Header.TLabel")
        title_lbl.pack(side=tk.LEFT)
        
        demo_btn = ttk.Button(header_frame, text="Chạy Thử Nghiệm (Demo Offline)", command=self.run_demo_mock, style="Accent.TButton")
        demo_btn.pack(side=tk.RIGHT, padx=5)

        # 2. Phân chia màn hình (Trái: Control, Phải: Kết quả & Logs)
        main_paned = ttk.PanedWindow(self.root, orient=tk.HORIZONTAL)
        main_paned.pack(fill=tk.BOTH, expand=True, padx=15, pady=10)
        
        left_container = ttk.Frame(main_paned, width=350)
        right_container = ttk.Frame(main_paned)
        
        main_paned.add(left_container, weight=1)
        main_paned.add(right_container, weight=3)
        
        # --- CỘT TRÁI: ĐIỀU KHIỂN & INPUTS ---
        # 2.1 Tham số cấu hình
        config_box = ttk.LabelFrame(left_container, text=" Cấu hình tham số kết nối ", padding=10)
        config_box.pack(fill=tk.X, pady=(0, 10))
        
        # Org ID
        ttk.Label(config_box, text="Org ID:").grid(row=0, column=0, sticky="w", pady=4)
        self.ent_org = ttk.Entry(config_box)
        self.ent_org.insert(0, "28753")
        self.ent_org.grid(row=0, column=1, sticky="ew", padx=5, pady=4)
        
        # User ID
        ttk.Label(config_box, text="User ID:").grid(row=1, column=0, sticky="w", pady=4)
        self.ent_user = ttk.Entry(config_box)
        self.ent_user.insert(0, "658359")
        self.ent_user.grid(row=1, column=1, sticky="ew", padx=5, pady=4)
        
        # Item Type
        ttk.Label(config_box, text="Item Type:").grid(row=2, column=0, sticky="w", pady=4)
        self.ent_type = ttk.Entry(config_box)
        self.ent_type.insert(0, "Q")
        self.ent_type.grid(row=2, column=1, sticky="ew", padx=5, pady=4)
        
        # Enabled Flag
        ttk.Label(config_box, text="Enabled:").grid(row=3, column=0, sticky="w", pady=4)
        self.ent_flag = ttk.Entry(config_box)
        self.ent_flag.insert(0, "Y")
        self.ent_flag.grid(row=3, column=1, sticky="ew", padx=5, pady=4)
        
        config_box.columnconfigure(1, weight=1)

        # 2.2 Danh sách đầu vào
        input_box = ttk.LabelFrame(left_container, text=" Danh sách mã Item Input (Mỗi dòng 1 mã) ", padding=10)
        input_box.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        
        self.txt_input = tk.Text(input_box, height=8, bg="#0f172a", fg="white", insertbackground="white", font=("Consolas", 10))
        self.txt_input.pack(fill=tk.BOTH, expand=True, side=tk.LEFT)
        self.txt_input.insert(tk.END, "QCAG26581\nQCAD17495")
        
        scr_input = ttk.Scrollbar(input_box, command=self.txt_input.yview)
        scr_input.pack(fill=tk.Y, side=tk.RIGHT)
        self.txt_input.config(yscrollcommand=scr_input.set)

        # 2.3 Cụm nút điều phối hành động
        action_box = ttk.LabelFrame(left_container, text=" Tiến trình điều phối ", padding=10)
        action_box.pack(fill=tk.X)
        
        self.btn_step1 = ttk.Button(action_box, text="Chạy Bước 1: Tra cứu mã", command=self.start_step1)
        self.btn_step1.pack(fill=tk.X, pady=4)
        
        self.btn_step2 = ttk.Button(action_box, text="Chạy Bước 2: Xem tồn kho", command=self.start_step2)
        self.btn_step2.pack(fill=tk.X, pady=4)
        
        self.btn_all = ttk.Button(action_box, text="Chạy tự động (Bước 1 + 2)", command=self.start_full_flow)
        self.btn_all.pack(fill=tk.X, pady=4)

        # Trạng thái & Tiến trình
        self.status_lbl = ttk.Label(left_container, text="Sẵn sàng.", foreground="#38bdf8", font=("Segoe UI", 9, "italic"))
        self.status_lbl.pack(fill=tk.X, pady=5)
        
        self.progress = ttk.Progressbar(left_container, mode="determinate")
        self.progress.pack(fill=tk.X, pady=2)

        # --- CỘT PHẢI: BẢNG KẾT QUẢ & LOGS ---
        self.notebook = ttk.Notebook(right_container)
        self.notebook.pack(fill=tk.BOTH, expand=True)
        
        # Tab 1: Bước 1 (ds_List)
        self.tab1 = ttk.Frame(self.notebook)
        self.notebook.add(self.tab1, text=" Bước 1: Tra cứu Item (ds_List) ")
        self.build_tab1_ui()
        
        # Tab 2: Bước 2 (ds_ohlist)
        self.tab2 = ttk.Frame(self.notebook)
        self.notebook.add(self.tab2, text=" Bước 2: Xem tồn kho (ds_ohlist) ")
        self.build_tab2_ui()
        
        # Tab 3: Logs XML Monitor
        self.tab3 = ttk.Frame(self.notebook)
        self.notebook.add(self.tab3, text=" Giám sát XML Request/Response ")
        self.build_tab3_ui()

    def build_tab1_ui(self):
        ctrl_frame = ttk.Frame(self.tab1)
        ctrl_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(ctrl_frame, text="Tìm nhanh:").pack(side=tk.LEFT, padx=5)
        self.search_val1 = tk.StringVar()
        self.search_val1.trace_add("write", lambda *args: self.filter_tree(self.tree1, self.search_val1.get(), self.step1_raw_data))
        ent_search = ttk.Entry(ctrl_frame, textvariable=self.search_val1, width=30)
        ent_search.pack(side=tk.LEFT, padx=5)
        
        btn_exp = ttk.Button(ctrl_frame, text="Xuất Báo cáo B1", command=lambda: self.export_results(1))
        btn_exp.pack(side=tk.RIGHT, padx=5)
        
        # Treeview B1
        tbl_frame = ttk.Frame(self.tab1)
        tbl_frame.pack(fill=tk.BOTH, expand=True)
        
        # Thiết kế các cột dữ liệu quan trọng cho Bước 1
        self.cols_step1 = ["uniqueNo", "item", "itemDescription", "primaryUomCode", "itemCost", "buyerName", "itemStatus", "inventoryItemId", "organizationId", "poType"]
        self.tree1 = ttk.Treeview(tbl_frame, columns=self.cols_step1, show="headings", selectmode="browse")
        
        # Đặt tiêu đề cột
        for col in self.cols_step1:
            self.tree1.heading(col, text=col)
            self.tree1.column(col, width=110, anchor="w")
            
        vsb = ttk.Scrollbar(tbl_frame, orient="vertical", command=self.tree1.yview)
        hsb = ttk.Scrollbar(tbl_frame, orient="horizontal", command=self.tree1.xview)
        self.tree1.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        hsb.pack(side=tk.BOTTOM, fill=tk.X)
        self.tree1.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

    def build_tab2_ui(self):
        ctrl_frame = ttk.Frame(self.tab2)
        ctrl_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(ctrl_frame, text="Tìm nhanh:").pack(side=tk.LEFT, padx=5)
        self.search_val2 = tk.StringVar()
        self.search_val2.trace_add("write", lambda *args: self.filter_tree(self.tree2, self.search_val2.get(), self.step2_raw_data))
        ent_search = ttk.Entry(ctrl_frame, textvariable=self.search_val2, width=30)
        ent_search.pack(side=tk.LEFT, padx=5)
        
        btn_exp = ttk.Button(ctrl_frame, text="Xuất Báo cáo B2", command=lambda: self.export_results(2))
        btn_exp.pack(side=tk.RIGHT, padx=5)
        
        # Treeview B2
        tbl_frame = ttk.Frame(self.tab2)
        tbl_frame.pack(fill=tk.BOTH, expand=True)
        
        self.cols_step2 = ["lpnId", "partNo", "makerModel", "maker", "subinventoryCode", "grade", "qty", "unitPriceNew", "receiptDate", "requestTeam", "requestor", "onhandAmount"]
        self.tree2 = ttk.Treeview(tbl_frame, columns=self.cols_step2, show="headings", selectmode="browse")
        
        for col in self.cols_step2:
            self.tree2.heading(col, text=col)
            self.tree2.column(col, width=110, anchor="w")
            
        vsb = ttk.Scrollbar(tbl_frame, orient="vertical", command=self.tree2.yview)
        hsb = ttk.Scrollbar(tbl_frame, orient="horizontal", command=self.tree2.xview)
        self.tree2.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        hsb.pack(side=tk.BOTTOM, fill=tk.X)
        self.tree2.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

    def build_tab3_ui(self):
        self.xml_logs = tk.Text(self.tab3, bg="#020617", fg="#34d399", font=("Consolas", 9), insertbackground="white")
        self.xml_logs.pack(fill=tk.BOTH, expand=True, side=tk.LEFT)
        
        scr_log = ttk.Scrollbar(self.tab3, command=self.xml_logs.yview)
        scr_log.pack(fill=tk.Y, side=tk.RIGHT)
        self.xml_logs.config(yscrollcommand=scr_log.set)

    # --- LOGIC ĐIỀU PHỐI EVENT ---
    def add_log(self, text):
        self.xml_logs.insert(tk.END, text + "\n" + "="*80 + "\n")
        self.xml_logs.see(tk.END)

    def filter_tree(self, tree, search_str, raw_data):
        # Làm sạch bảng
        for item in tree.get_children():
            tree.delete(item)
            
        search_str = search_str.lower()
        for row in raw_data:
            # Kiểm tra xem có trường nào khớp chuỗi tìm kiếm không
            match = False
            if not search_str:
                match = True
            else:
                for val in row.values():
                    if search_str in str(val).lower():
                        match = True
                        break
            if match:
                # Đưa đúng thứ tự các cột vào
                columns = tree["columns"]
                row_values = [row.get(col, "") for col in columns]
                tree.insert("", "end", values=row_values)

    def fill_tree_data(self, tree, raw_data):
        columns = tree["columns"]
        for item in tree.get_children():
            tree.delete(item)
        for row in raw_data:
            row_values = [row.get(col, "") for col in columns]
            tree.insert("", "end", values=row_values)

    def get_input_list(self):
        lines = self.txt_input.get("1.0", tk.END).strip().splitlines()
        # Loại bỏ khoảng trắng thừa hoặc dòng trống
        return [l.strip() for l in lines if l.strip()]

    def set_gui_state(self, is_enabled):
        state = tk.NORMAL if is_enabled else tk.DISABLED
        self.btn_step1.config(state=state)
        self.btn_step2.config(state=state)
        self.btn_all.config(state=state)

    # --- CHẠY GIẢ LẬP DEMO (MOCK MODE) ---
    def run_demo_mock(self):
        self.step1_raw_data = parse_nexacro_xml(MOCK_XML_STEP1, "ds_List")
        self.step2_raw_data = parse_nexacro_xml(MOCK_XML_STEP2, "ds_ohlist")
        
        self.fill_tree_data(self.tree1, self.step1_raw_data)
        self.fill_tree_data(self.tree2, self.step2_raw_data)
        
        self.add_log("[MOCK MODE] Sử dụng XML phản hồi giả định cho Bước 1:")
        self.add_log(MOCK_XML_STEP1)
        self.add_log("[MOCK MODE] Sử dụng XML phản hồi giả định cho Bước 2:")
        self.add_log(MOCK_XML_STEP2)
        
        self.notebook.select(self.tab2)
        self.status_lbl.config(text="Đã giả lập nạp dữ liệu Demo thành công!", foreground="#10b981")
        messagebox.showinfo("Thành công", "Đã nạp dữ liệu giả lập mẫu. Bạn có thể xem kết quả ở Tab 1 và Tab 2 để kiểm chứng!")

    # --- ĐA LUỒNG BACKEND PROCESS ---
    def process_queue(self):
        try:
            while True:
                task_type, payload = self.gui_queue.get_nowait()
                if task_type == "STATUS":
                    self.status_lbl.config(text=payload[0], foreground=payload[1])
                elif task_type == "PROGRESS":
                    self.progress['value'] = payload
                elif task_type == "LOG":
                    self.add_log(payload)
                elif task_type == "DONE_STEP1":
                    self.step1_raw_data = payload
                    self.fill_tree_data(self.tree1, self.step1_raw_data)
                    self.notebook.select(self.tab1)
                    self.set_gui_state(True)
                elif task_type == "DONE_STEP2":
                    self.step2_raw_data = payload
                    self.fill_tree_data(self.tree2, self.step2_raw_data)
                    self.notebook.select(self.tab2)
                    self.set_gui_state(True)
                elif task_type == "ERROR_POP":
                    messagebox.showerror("Lỗi kết nối", payload)
                    self.set_gui_state(True)
                self.gui_queue.task_done()
        except queue.Empty:
            pass
        self.root.after(100, self.process_queue)

    def start_step1(self):
        self.set_gui_state(False)
        threading.Thread(target=self.run_step1_thread, daemon=True).start()

    def start_step2(self):
        self.set_gui_state(False)
        threading.Thread(target=self.run_step2_thread, daemon=True).start()

    def start_full_flow(self):
        self.set_gui_state(False)
        threading.Thread(target=self.run_full_flow_thread, daemon=True).start()

    # --- CÁC HÀM XỬ LÝ TRONG LUỒNG RIÊNG (THREAD) ---
    def run_step1_thread(self):
        items = self.get_input_list()
        if not items:
            self.gui_queue.put(("ERROR_POP", "Danh sách Item nhập vào trống!"))
            return
            
        org_id = self.ent_org.get().strip()
        item_type = self.ent_type.get().strip()
        enabled_flag = self.ent_flag.get().strip()
        
        results = []
        total = len(items)
        url = "http://ems.lgdisplay.com:9000/emscom/popupNav/retrieveItem.lgdn"
        headers = {'Content-Type': 'text/xml; charset=utf-8'}
        
        for index, item in enumerate(items):
            progress_val = int(((index + 1) / total) * 100)
            self.gui_queue.put(("STATUS", (f"B1: Đang tra cứu {item} ({index+1}/{total})...", "#38bdf8")))
            self.gui_queue.put(("PROGRESS", progress_val))
            
            payload = build_xml_step1(item, item_type, org_id, enabled_flag)
            self.gui_queue.put(("LOG", f"GỬI REQUEST BƯỚC 1 (Mã: {item}):\n{payload}"))
            
            try:
                res = requests.post(url, data=payload, headers=headers, timeout=10)
                if res.status_code == 200:
                    self.gui_queue.put(("LOG", f"PHẢN HỒI BƯỚC 1 (Mã: {item}):\n{res.text}"))
                    records = parse_nexacro_xml(res.text, "ds_List")
                    results.extend(records)
                else:
                    self.gui_queue.put(("LOG", f"LỖI HTTP {res.status_code} với mã {item}"))
            except Exception as e:
                self.gui_queue.put(("LOG", f"THẤT BẠI khi kết nối máy chủ cho mã {item}: {e}"))
                
        self.gui_queue.put(("STATUS", (f"Hoàn thành B1! Đã bóc tách {len(results)} bản ghi.", "#10b981")))
        self.gui_queue.put(("DONE_STEP1", results))

    def run_step2_thread(self):
        if not self.step1_raw_data:
            self.gui_queue.put(("ERROR_POP", "Vui lòng hoàn thành Bước 1 trước để lấy Inventory Item ID!"))
            return
            
        org_id = self.ent_org.get().strip()
        user_id = self.ent_user.get().strip()
        
        # Nhặt tập hợp các cặp (org_id, inventory_item_id) độc bản tránh lặp
        query_pairs = []
        seen = set()
        for row in self.step1_raw_data:
            o_id = row.get("organizationId", org_id)
            inv_id = row.get("inventoryItemId")
            if inv_id and (o_id, inv_id) not in seen:
                seen.add((o_id, inv_id))
                query_pairs.append((o_id, inv_id))
                
        if not query_pairs:
            self.gui_queue.put(("ERROR_POP", "Không tìm thấy inventoryItemId hợp lệ từ Bước 1!"))
            return
            
        results = []
        total = len(query_pairs)
        url = "http://ems.lgdisplay.com:9000/emswonew/wn30/wn360Nav/WN360WoSaveInit.lgdn"
        headers = {'Content-Type': 'text/xml; charset=utf-8'}
        
        for index, (o_id, inv_id) in enumerate(query_pairs):
            progress_val = int(((index + 1) / total) * 100)
            self.gui_queue.put(("STATUS", (f"B2: Đang tải tồn kho cho ID {inv_id} ({index+1}/{total})...", "#38bdf8")))
            self.gui_queue.put(("PROGRESS", progress_val))
            
            payload = build_xml_step2(o_id, user_id, inv_id)
            self.gui_queue.put(("LOG", f"GỬI REQUEST BƯỚC 2 (Inventory ID: {inv_id}):\n{payload}"))
            
            try:
                res = requests.post(url, data=payload, headers=headers, timeout=10)
                if res.status_code == 200:
                    self.gui_queue.put(("LOG", f"PHẢN HỒI BƯỚC 2 (Inventory ID: {inv_id}):\n{res.text}"))
                    records = parse_nexacro_xml(res.text, "ds_ohlist")
                    results.extend(records)
                else:
                    self.gui_queue.put(("LOG", f"LỖI HTTP {res.status_code} với ID {inv_id}"))
            except Exception as e:
                self.gui_queue.put(("LOG", f"THẤT BẠI khi kết nối máy chủ cho ID {inv_id}: {e}"))
                
        self.gui_queue.put(("STATUS", (f"Hoàn thành B2! Đã bóc tách {len(results)} lô tồn kho.", "#10b981")))
        self.gui_queue.put(("DONE_STEP2", results))

    def run_full_flow_thread(self):
        # 1. Chạy tiến trình Bước 1 trước
        items = self.get_input_list()
        if not items:
            self.gui_queue.put(("ERROR_POP", "Danh sách Item nhập vào trống!"))
            return
            
        org_id = self.ent_org.get().strip()
        item_type = self.ent_type.get().strip()
        enabled_flag = self.ent_flag.get().strip()
        user_id = self.ent_user.get().strip()
        
        step1_results = []
        total1 = len(items)
        url1 = "http://ems.lgdisplay.com:9000/emscom/popupNav/retrieveItem.lgdn"
        headers = {'Content-Type': 'text/xml; charset=utf-8'}
        
        for index, item in enumerate(items):
            progress_val = int(((index + 1) / total1) * 50) # 50% đầu dành cho B1
            self.gui_queue.put(("STATUS", (f"[1/2] Tra cứu mã {item} ({index+1}/{total1})...", "#f59e0b")))
            self.gui_queue.put(("PROGRESS", progress_val))
            
            payload = build_xml_step1(item, item_type, org_id, enabled_flag)
            try:
                res = requests.post(url1, data=payload, headers=headers, timeout=10)
                if res.status_code == 200:
                    step1_results.extend(parse_nexacro_xml(res.text, "ds_List"))
            except Exception as e:
                self.gui_queue.put(("LOG", f"B1 Lỗi kết nối {item}: {e}"))
                
        self.gui_queue.put(("DONE_STEP1", step1_results))
        
        # 2. Chuyển tiếp kết quả sang Bước 2 ngay lập tức
        query_pairs = []
        seen = set()
        for row in step1_results:
            o_id = row.get("organizationId", org_id)
            inv_id = row.get("inventoryItemId")
            if inv_id and (o_id, inv_id) not in seen:
                seen.add((o_id, inv_id))
                query_pairs.append((o_id, inv_id))
                
        if not query_pairs:
            self.gui_queue.put(("STATUS", ("Xong Bước 1 nhưng không tìm thấy Inventory ID để chạy Bước 2.", "#f43f5e")))
            self.gui_queue.put(("PROGRESS", 100))
            self.gui_queue.put(("DONE_STEP2", []))
            return
            
        step2_results = []
        total2 = len(query_pairs)
        url2 = "http://ems.lgdisplay.com:9000/emswonew/wn30/wn360Nav/WN360WoSaveInit.lgdn"
        
        for index, (o_id, inv_id) in enumerate(query_pairs):
            # 50% sau dành cho B2
            progress_val = 50 + int(((index + 1) / total2) * 50)
            self.gui_queue.put(("STATUS", (f"[2/2] Tải tồn kho ID {inv_id} ({index+1}/{total2})...", "#34d399")))
            self.gui_queue.put(("PROGRESS", progress_val))
            
            payload = build_xml_step2(o_id, user_id, inv_id)
            try:
                res = requests.post(url2, data=payload, headers=headers, timeout=10)
                if res.status_code == 200:
                    step2_results.extend(parse_nexacro_xml(res.text, "ds_ohlist"))
            except Exception as e:
                self.gui_queue.put(("LOG", f"B2 Lỗi kết nối {inv_id}: {e}"))
                
        self.gui_queue.put(("STATUS", (f"Hoàn thành tự động cả 2 bước thành công!", "#10b981")))
        self.gui_queue.put(("PROGRESS", 100))
        self.gui_queue.put(("DONE_STEP2", step2_results))

    # --- XUẤT FILE KẾT QUẢ ---
    def export_results(self, step_number):
        data = self.step1_raw_data if step_number == 1 else self.step2_raw_data
        if not data:
            messagebox.showwarning("Dữ liệu trống", "Không có dữ liệu để xuất file!")
            return
            
        # Ưu tiên lưu .xlsx nếu có pandas, ngược lại dùng .csv chuẩn
        file_types = [("Excel Files", "*.xlsx"), ("CSV Files", "*.csv")] if HAS_PANDAS else [("CSV Files", "*.csv")]
        default_ext = ".xlsx" if HAS_PANDAS else ".csv"
        
        file_path = filedialog.asksaveasfilename(
            defaultextension=default_ext,
            filetypes=file_types,
            title=f"Lưu kết quả Bước {step_number}"
        )
        if not file_path:
            return
            
        try:
            if file_path.endswith('.xlsx') and HAS_PANDAS:
                df = pd.DataFrame(data)
                df.to_excel(file_path, index=False)
            else:
                # Ghi file CSV xử lý tiếng Hàn/Tiếng Việt chuẩn xác bằng bộ giải mã utf-8-sig
                with open(file_path, mode='w', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, fieldnames=data[0].keys())
                    writer.writeheader()
                    writer.writerows(data)
            messagebox.showinfo("Xuất file thành công", f"Dữ liệu đã được xuất thành công ra đường dẫn:\n{file_path}")
        except Exception as e:
            messagebox.showerror("Lỗi xuất file", f"Đã xảy ra lỗi khi tạo file báo cáo:\n{e}")

if __name__ == "__main__":
    root = tk.Tk()
    app = EMSToolApp(root)
    root.mainloop()
