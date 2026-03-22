#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Flink State Backends 模块源码分析 - PDF 生成脚本"""

import os, math
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm, cm
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, HRFlowable,
)
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.graphics.shapes import Drawing, Line, Rect, String, Polygon

# === 字体 ===
FONT_CN = "Helvetica"; FONT_CN_B = "Helvetica-Bold"
for fp in ["/System/Library/Fonts/PingFang.ttc","/System/Library/Fonts/STHeiti Light.ttc",
           "/System/Library/Fonts/Hiragino Sans GB.ttc","/Library/Fonts/Arial Unicode.ttf"]:
    if os.path.exists(fp):
        try:
            pdfmetrics.registerFont(TTFont("CN",fp,subfontIndex=0) if fp.endswith(".ttc") else TTFont("CN",fp))
            FONT_CN=FONT_CN_B="CN"; break
        except: continue

def mkstyles():
    s=getSampleStyleSheet()
    def a(n,**kw): s.add(ParagraphStyle(name=n,**kw))
    a("T",fontName=FONT_CN_B,fontSize=26,leading=36,alignment=TA_CENTER,spaceAfter=20,textColor=colors.HexColor("#1a237e"))
    a("ST",fontName=FONT_CN,fontSize=14,leading=20,alignment=TA_CENTER,spaceAfter=10,textColor=colors.HexColor("#455a64"))
    a("H1",fontName=FONT_CN_B,fontSize=20,leading=28,spaceBefore=24,spaceAfter=12,textColor=colors.HexColor("#1565c0"))
    a("H2",fontName=FONT_CN_B,fontSize=16,leading=22,spaceBefore=16,spaceAfter=8,textColor=colors.HexColor("#1976d2"))
    a("H3",fontName=FONT_CN_B,fontSize=13,leading=18,spaceBefore=12,spaceAfter=6,textColor=colors.HexColor("#1e88e5"))
    a("B",fontName=FONT_CN,fontSize=10.5,leading=17,spaceBefore=4,spaceAfter=4,alignment=TA_JUSTIFY,textColor=colors.HexColor("#212121"))
    a("BI",fontName=FONT_CN,fontSize=10.5,leading=17,spaceBefore=2,spaceAfter=2,leftIndent=20,textColor=colors.HexColor("#212121"))
    a("BL",fontName=FONT_CN,fontSize=10.5,leading=17,spaceBefore=2,spaceAfter=2,leftIndent=30,bulletIndent=15,textColor=colors.HexColor("#212121"))
    a("Cap",fontName=FONT_CN,fontSize=9,leading=13,alignment=TA_CENTER,spaceAfter=12,textColor=colors.HexColor("#757575"))
    return s

# === 绘图工具 ===
def box(d,x,y,w,h,txt,fc,tc=colors.white,fs=9):
    d.add(Rect(x,y,w,h,rx=5,ry=5,fillColor=fc,strokeColor=colors.HexColor("#90a4ae"),strokeWidth=0.5))
    lines=txt.split("\n")
    sy=y+h/2+(len(lines)*(fs+2))/2-fs/2
    for i,l in enumerate(lines): d.add(String(x+w/2,sy-i*(fs+2),l,fontName=FONT_CN,fontSize=fs,fillColor=tc,textAnchor="middle"))

def arrow(d,x1,y1,x2,y2,sc=colors.HexColor("#546e7a"),sw=1.2):
    d.add(Line(x1,y1,x2,y2,strokeColor=sc,strokeWidth=sw))
    a_=math.atan2(y2-y1,x2-x1); al=6; aa=math.pi/7
    d.add(Polygon([x2,y2,x2-al*math.cos(a_-aa),y2-al*math.sin(a_-aa),x2-al*math.cos(a_+aa),y2-al*math.sin(a_+aa)],fillColor=sc,strokeColor=sc,strokeWidth=0.5))

def label(d,x,y,t,fs=8,c=colors.HexColor("#455a64")):
    d.add(String(x,y,t,fontName=FONT_CN,fontSize=fs,fillColor=c,textAnchor="middle"))

def mktable(headers,rows,cw):
    t=Table([headers]+rows,colWidths=cw)
    t.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,0),colors.HexColor("#1565c0")),("TEXTCOLOR",(0,0),(-1,0),colors.white),
        ("FONTNAME",(0,0),(-1,-1),FONT_CN),("FONTSIZE",(0,0),(-1,-1),8),("ALIGN",(0,0),(-1,-1),"LEFT"),
        ("GRID",(0,0),(-1,-1),0.5,colors.HexColor("#b0bec5")),("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.HexColor("#fafafa"),colors.HexColor("#f5f5f5")]),
        ("VALIGN",(0,0),(-1,-1),"MIDDLE"),("TOPPADDING",(0,0),(-1,-1),4),("BOTTOMPADDING",(0,0),(-1,-1),4)]))
    return t

# === 图1: 架构总览 ===
def fig_arch():
    d=Drawing(500,460)
    d.add(Rect(0,0,500,460,fillColor=colors.HexColor("#fafafa"),strokeColor=colors.HexColor("#e0e0e0"),strokeWidth=0.5))
    label(d,250,440,"Flink State Backends 架构总览",13,colors.HexColor("#1565c0"))
    box(d,130,400,240,28,"StateBackend (用户配置入口)",colors.HexColor("#0d47a1"),fs=9)
    bks=[("RocksDB","#e65100",10),("ForSt","#2e7d32",110),("Changelog","#6a1b9a",210),("HeapSpill","#00695c",310),("Common","#c62828",410)]
    for t,c,x in bks:
        box(d,x,350,85,35,t+"\nBackend",colors.HexColor(c),fs=7); arrow(d,x+42,400,x+42,385,colors.HexColor("#90caf9"),0.8)
    label(d,250,325,"KeyedStateBackend 实现层",9,colors.HexColor("#455a64"))
    impls=[("RocksDBKeyed\nStateBackend","#e65100",10),("ForStKeyed\nStateBackend","#2e7d32",110),("ChangelogKeyed\nStateBackend","#6a1b9a",210),("CopyOnWrite\nSkipListStateMap","#00695c",310),("Materialization\nTarget","#c62828",410)]
    for t,c,x in impls:
        box(d,x,270,85,40,t,colors.HexColor(c),fs=6); arrow(d,x+42,350,x+42,310,colors.HexColor("#b0bec5"),0.8)
    label(d,250,245,"快照策略层",9,colors.HexColor("#455a64"))
    snaps=[("FullSnapshot","#e65100",10),("Incremental\nSnapshot","#f57c00",110),("ForSt\nIncremental","#2e7d32",210),("Changelog\nPersist","#6a1b9a",310),("Periodic\nMaterialization","#c62828",410)]
    for t,c,x in snaps:
        box(d,x,190,85,40,t,colors.HexColor(c),fs=6); arrow(d,x+42,270,x+42,230,colors.HexColor("#b0bec5"),0.8)
    label(d,250,165,"恢复操作层",9,colors.HexColor("#455a64"))
    rsts=[("NoneRestore","#e65100",10),("FullRestore","#f57c00",80),("Incremental\nRestore","#fb8c00",155),("HeapTimers\nRestore","#ff9800",235),("ForStRestore","#2e7d32",315),("Changelog\nRestore","#6a1b9a",395)]
    for t,c,x in rsts:
        box(d,x,110,72,40,t,colors.HexColor(c),fs=6)
    label(d,250,85,"底层存储引擎",9,colors.HexColor("#455a64"))
    for t,c,x in [("RocksDB JNI (Native C++)","#37474f",30),("ForSt JNI (Next-Gen)","#37474f",180),("Off-Heap (Allocator)","#37474f",340)]:
        box(d,x,35,140,35,t,colors.HexColor(c),fs=7)
    label(d,250,15,"RocksDB:LSM-Tree | ForSt:远程存储+异步IO | HeapSpill:SkipList+COW",7,colors.HexColor("#9e9e9e"))
    return d

# === 图2: RocksDB 初始化流程 ===
def fig_rocksdb_init():
    d=Drawing(500,530)
    d.add(Rect(0,0,500,530,fillColor=colors.HexColor("#fafafa"),strokeColor=colors.HexColor("#e0e0e0"),strokeWidth=0.5))
    label(d,250,508,"RocksDB State Backend 初始化流程",13,colors.HexColor("#1565c0"))
    bw,bh,cx=165,26,50
    steps=[("1. createKeyedStateBackend()","#0d47a1"),("2. 加载JNI原生库(Native Lib)","#1565c0"),
           ("3. 初始化本地数据目录","#1976d2"),("4. 创建RocksDBResourceContainer","#1e88e5"),
           ("5. 创建WriteBufferMgr+BlockCache","#2196f3"),("6. 创建Builder(Builder模式)","#42a5f5"),
           ("7. 选择恢复策略(None/Full/Inc)","#1976d2"),("8. 执行恢复操作(openDB)","#1565c0"),
           ("9. 初始化快照策略(Full/Inc)","#0d47a1"),("10. 创建优先级队列工厂","#0d47a1"),
           ("11. 返回RocksDBKeyedStateBackend","#1a237e")]
    notes=["ConfigurableStateBackend","重试+Fallback","tempDir/dbPath","DBOptions/CFOptions",
           "内存预算控制","Builder模式组装","根据StateHandle类型","openDB+ColumnFamily",
           "根据incremental配置","RocksDB/Heap Timer","就绪可服务"]
    for i,(s,c) in enumerate(steps):
        y=465-i*42
        box(d,cx,y,bw,bh,s,colors.HexColor(c),fs=7)
        if i>0: arrow(d,cx+bw/2,y+42,cx+bw/2,y+bh)
        box(d,cx+bw+25,y,115,bh,notes[i],colors.HexColor("#e3f2fd"),colors.HexColor("#1565c0"),7)
        arrow(d,cx+bw,y+bh/2,cx+bw+25,y+bh/2,colors.HexColor("#90caf9"),0.8)
    return d

# === 图3: Checkpoint流程 ===
def fig_checkpoint():
    d=Drawing(500,460)
    d.add(Rect(0,0,500,460,fillColor=colors.HexColor("#fafafa"),strokeColor=colors.HexColor("#e0e0e0"),strokeWidth=0.5))
    label(d,250,438,"RocksDB Checkpoint 流程 (全量 vs 增量)",13,colors.HexColor("#1565c0"))
    label(d,115,410,"Full Snapshot",10,colors.HexColor("#e65100"))
    fsteps=[("Checkpoint触发","#e65100"),("snapshot()调用","#f57c00"),("RocksNativeFullSnapshot\nStrategy","#fb8c00"),
            ("getSnapshot()一致性视图","#ff9800"),("遍历CF Iterator读KV","#ffb74d"),("写入CheckpointStream","#ffa726"),("释放Snapshot+返回Handle","#f57c00")]
    for i,(t,c) in enumerate(fsteps):
        y=375-i*50; box(d,20,y,145,32,t,colors.HexColor(c),fs=7)
        if i>0: arrow(d,92,y+50,92,y+32)
    label(d,375,410,"Incremental Snapshot",10,colors.HexColor("#1565c0"))
    isteps=[("Checkpoint触发","#0d47a1"),("snapshot()调用","#1565c0"),("RocksIncremental\nSnapshotStrategy","#1976d2"),
            ("flush ColumnFamilies","#1e88e5"),("获取LiveFiles计算增量","#2196f3"),("上传新增SST文件","#42a5f5"),("返回IncrementalHandle","#1565c0")]
    for i,(t,c) in enumerate(isteps):
        y=375-i*50; box(d,290,y,145,32,t,colors.HexColor(c),fs=7)
        if i>0: arrow(d,362,y+50,362,y+32)
    label(d,230,240,"增量:只上传新增SST",7,colors.HexColor("#9e9e9e"))
    label(d,230,180,"全量:遍历全部KV",7,colors.HexColor("#9e9e9e"))
    return d

# === 图4: 恢复选择 ===
def fig_restore():
    d=Drawing(500,400)
    d.add(Rect(0,0,500,400,fillColor=colors.HexColor("#fafafa"),strokeColor=colors.HexColor("#e0e0e0"),strokeWidth=0.5))
    label(d,250,378,"RocksDB 恢复操作选择流程",13,colors.HexColor("#1565c0"))
    box(d,150,330,200,28,"getRocksDBRestoreOperation()",colors.HexColor("#0d47a1"),fs=8)
    box(d,175,275,150,28,"StateHandles == empty?",colors.HexColor("#ff8f00"),colors.white,fs=8)
    arrow(d,250,330,250,303)
    box(d,20,220,130,28,"NoneRestoreOperation",colors.HexColor("#2e7d32"),fs=8)
    arrow(d,175,289,150,248,colors.HexColor("#66bb6a")); label(d,140,275,"Yes",8,colors.HexColor("#2e7d32"))
    box(d,230,220,150,28,"IncrementalHandle?",colors.HexColor("#ff8f00"),colors.white,fs=8)
    arrow(d,325,275,305,248,colors.HexColor("#e65100")); label(d,335,265,"No",8,colors.HexColor("#e65100"))
    box(d,350,165,140,28,"IncrementalRestore",colors.HexColor("#1565c0"),fs=8)
    arrow(d,380,220,420,193,colors.HexColor("#42a5f5")); label(d,415,210,"Yes",8,colors.HexColor("#1565c0"))
    box(d,150,165,140,28,"HeapTimers+Full?",colors.HexColor("#ff8f00"),colors.white,fs=8)
    arrow(d,230,220,220,193,colors.HexColor("#e65100")); label(d,205,210,"No",8,colors.HexColor("#e65100"))
    box(d,20,110,150,28,"HeapTimersFullRestore",colors.HexColor("#6a1b9a"),fs=8)
    arrow(d,150,165,95,138,colors.HexColor("#ab47bc")); label(d,100,155,"Yes",8,colors.HexColor("#6a1b9a"))
    box(d,280,110,140,28,"FullRestoreOperation",colors.HexColor("#e65100"),fs=8)
    arrow(d,290,165,350,138,colors.HexColor("#ef5350")); label(d,340,155,"No",8,colors.HexColor("#e65100"))
    for i,t in enumerate(["None:创建空DB","Full:反序列化全量KV写入","Incremental:下载SST+Ingest","HeapTimers:全量+Timer分离到Heap"]):
        label(d,250,75-i*16,t,7,colors.HexColor("#616161"))
    return d

# === 图5: ForSt异步执行 ===
def fig_forst():
    d=Drawing(500,430)
    d.add(Rect(0,0,500,430,fillColor=colors.HexColor("#fafafa"),strokeColor=colors.HexColor("#e0e0e0"),strokeWidth=0.5))
    label(d,250,408,"ForSt 异步状态执行流程 (StateExecutor)",13,colors.HexColor("#1565c0"))
    box(d,150,360,200,28,"StateRequestContainer(批量请求)",colors.HexColor("#0d47a1"),fs=8)
    d.add(Rect(25,85,450,260,rx=8,ry=8,fillColor=colors.HexColor("#e8f5e9"),strokeColor=colors.HexColor("#2e7d32"),strokeWidth=1))
    label(d,250,325,"ForStStateExecutor",11,colors.HexColor("#2e7d32"))
    box(d,160,290,180,28,"executeBatchRequests() 请求分类",colors.HexColor("#2e7d32"),fs=8)
    arrow(d,250,360,250,318)
    box(d,35,230,120,32,"Get请求\nMultiGetOperation",colors.HexColor("#1565c0"),fs=7)
    box(d,185,230,130,32,"Iterate请求\nIterateOperation",colors.HexColor("#6a1b9a"),fs=7)
    box(d,345,230,120,32,"Put请求\nWriteBatchOp",colors.HexColor("#e65100"),fs=7)
    arrow(d,160,298,95,262,colors.HexColor("#1565c0")); arrow(d,250,290,250,262,colors.HexColor("#6a1b9a")); arrow(d,340,298,405,262,colors.HexColor("#e65100"))
    box(d,35,165,120,32,"Read线程池\n(readIOThreadNum)",colors.HexColor("#1976d2"),fs=7)
    box(d,185,165,130,32,"Read线程池\n(readIOThreadNum)",colors.HexColor("#7b1fa2"),fs=7)
    box(d,345,165,120,32,"Write线程池\n(writeIOThreadNum)",colors.HexColor("#bf360c"),fs=7)
    arrow(d,95,230,95,197,colors.HexColor("#42a5f5")); arrow(d,250,230,250,197,colors.HexColor("#ab47bc")); arrow(d,405,230,405,197,colors.HexColor("#ef5350"))
    box(d,120,100,260,28,"ForSt (RocksDB-Next-Gen) Native 引擎",colors.HexColor("#37474f"),fs=9)
    arrow(d,95,165,200,128,colors.HexColor("#546e7a")); arrow(d,250,165,250,128,colors.HexColor("#546e7a")); arrow(d,405,165,320,128,colors.HexColor("#546e7a"))
    box(d,130,35,240,28,"callbackRunner → Coordinator线程回调",colors.HexColor("#0d47a1"),fs=8)
    arrow(d,250,100,250,63)
    label(d,250,15,"读写IO线程分离 | 批量MultiGet | WriteBatch原子写入",7,colors.HexColor("#9e9e9e"))
    return d

# === 图6: Changelog物化 ===
def fig_changelog():
    d=Drawing(500,460)
    d.add(Rect(0,0,500,460,fillColor=colors.HexColor("#fafafa"),strokeColor=colors.HexColor("#e0e0e0"),strokeWidth=0.5))
    label(d,250,438,"Changelog State Backend 物化循环流程",13,colors.HexColor("#1565c0"))
    box(d,25,395,130,28,"1.状态变更操作\n(put/remove)",colors.HexColor("#6a1b9a"),fs=7)
    box(d,185,395,140,28,"2.记录到Changelog\nWriter",colors.HexColor("#7b1fa2"),fs=7)
    box(d,355,395,130,28,"3.委托底层\nBackend写入",colors.HexColor("#8e24aa"),fs=7)
    arrow(d,155,409,185,409,colors.HexColor("#ab47bc")); arrow(d,325,409,355,409,colors.HexColor("#ab47bc"))
    label(d,250,368,"Checkpoint触发时:",10,colors.HexColor("#1565c0"))
    box(d,40,325,190,28,"4.snapshot()→writer.persist()",colors.HexColor("#1565c0"),fs=8)
    box(d,270,325,190,28,"5.构建ChangelogStateHandle",colors.HexColor("#1976d2"),fs=8)
    arrow(d,230,339,270,339,colors.HexColor("#42a5f5"))
    box(d,80,270,340,30,"ChangelogSnapshotState: 已物化快照 + 未物化Changelog",colors.HexColor("#0d47a1"),fs=8)
    arrow(d,370,325,250,300,colors.HexColor("#1976d2"))
    label(d,250,242,"定期物化循环 (PeriodicMaterializationManager)",10,colors.HexColor("#e65100"))
    box(d,20,195,140,32,"6.定时触发\ntriggerMaterialization",colors.HexColor("#e65100"),fs=7)
    box(d,180,195,140,32,"7.initMaterialization\n底层backend snapshot",colors.HexColor("#f57c00"),fs=7)
    box(d,340,195,140,32,"8.异步上传\n物化数据",colors.HexColor("#fb8c00"),fs=7)
    arrow(d,160,211,180,211,colors.HexColor("#f57c00")); arrow(d,320,211,340,211,colors.HexColor("#fb8c00"))
    box(d,70,130,160,28,"9.handleResult\n更新lastMaterialized",colors.HexColor("#2e7d32"),fs=7)
    box(d,270,130,160,28,"10.截断旧Changelog\n释放已物化日志",colors.HexColor("#388e3c"),fs=7)
    arrow(d,410,195,345,158,colors.HexColor("#66bb6a")); arrow(d,230,144,270,144,colors.HexColor("#66bb6a"))
    box(d,70,75,160,28,"物化失败处理:\nallowedNumberOfFailures",colors.HexColor("#c62828"),fs=7)
    box(d,270,75,160,28,"超过容忍次数:\nTask失败",colors.HexColor("#b71c1c"),fs=7)
    arrow(d,150,130,150,103,colors.HexColor("#ef5350")); arrow(d,230,89,270,89,colors.HexColor("#ef5350"))
    label(d,250,45,"随机初始延迟避免惊群 | 物化将Changelog持久化为底层完整快照",7,colors.HexColor("#9e9e9e"))
    return d

# === 图7: COW SkipList ===
def fig_skiplist():
    d=Drawing(500,380)
    d.add(Rect(0,0,500,380,fillColor=colors.HexColor("#fafafa"),strokeColor=colors.HexColor("#e0e0e0"),strokeWidth=0.5))
    label(d,250,358,"CopyOnWrite SkipList StateMap 工作原理",13,colors.HexColor("#1565c0"))
    label(d,130,330,"SkipList 多层索引",10,colors.HexColor("#2e7d32"))
    levs=[("L3",305,[50,350]),("L2",278,[50,200,350]),("L1",251,[50,125,200,275,350]),("L0",224,[50,125,200,275,350,425])]
    for lb,y,ns in levs:
        label(d,20,y+2,lb,6,colors.HexColor("#9e9e9e"))
        for i,x in enumerate(ns):
            d.add(Rect(x,y,30,14,fillColor=colors.HexColor("#e3f2fd"),strokeColor=colors.HexColor("#1976d2"),strokeWidth=0.5,rx=2,ry=2))
            if i<len(ns)-1: arrow(d,x+30,y+7,ns[i+1],y+7,colors.HexColor("#42a5f5"),0.6)
    label(d,380,330,"COW机制",10,colors.HexColor("#e65100"))
    box(d,310,295,80,24,"Snapshot V1\n(只读)",colors.HexColor("#e65100"),fs=7)
    box(d,410,295,80,24,"Current V2\n(可写)",colors.HexColor("#2e7d32"),fs=7)
    label(d,250,195,"写入时 Copy-on-Write 流程",10,colors.HexColor("#455a64"))
    box(d,20,145,150,32,"node.version <\nhighestSnapshot?",colors.HexColor("#f57c00"),fs=7)
    box(d,20,90,150,32,"是:CopyOnWrite\n创建新节点+版本链",colors.HexColor("#e65100"),fs=7)
    box(d,280,145,160,32,"否:Replace\n直接替换无需COW",colors.HexColor("#2e7d32"),fs=7)
    box(d,280,90,160,32,"pruneTimestamp\n快照完成后清理旧版本",colors.HexColor("#388e3c"),fs=7)
    arrow(d,95,145,95,122,colors.HexColor("#e65100")); arrow(d,170,161,280,161,colors.HexColor("#2e7d32")); arrow(d,360,145,360,122,colors.HexColor("#388e3c"))
    label(d,250,55,"Off-Heap 内存管理",10,colors.HexColor("#455a64"))
    for t,c,x in [("Allocator","#00695c",30),("Chunk(1MB)","#00796b",150),("Node/Value","#00897b",280),("SpaceAlloc","#009688",400)]:
        box(d,x,15,100,26,t,colors.HexColor(c),fs=7)
    arrow(d,130,28,150,28,colors.HexColor("#4db6ac"),0.8); arrow(d,250,28,280,28,colors.HexColor("#4db6ac"),0.8); arrow(d,380,28,400,28,colors.HexColor("#4db6ac"),0.8)
    return d

# === 图8: 资源容器配置 ===
def fig_resource():
    d=Drawing(500,340)
    d.add(Rect(0,0,500,340,fillColor=colors.HexColor("#fafafa"),strokeColor=colors.HexColor("#e0e0e0"),strokeWidth=0.5))
    label(d,250,318,"RocksDB 资源容器配置层次",13,colors.HexColor("#1565c0"))
    layers=[("用户RocksDBOptionsFactory","#0d47a1",270,"最高优先级"),("Flink ConfigurableOptions","#1565c0",225,"中优先级"),
            ("PredefinedOptions模板","#1976d2",180,"基础优先级"),("RocksDB Default默认值","#2196f3",135,"兜底默认")]
    for t,c,y,n in layers:
        box(d,25,y,180,32,t,colors.HexColor(c),fs=8); box(d,230,y,120,32,n,colors.HexColor("#e3f2fd"),colors.HexColor("#1565c0"),7)
        arrow(d,205,y+16,230,y+16,colors.HexColor("#90caf9"),0.8)
    for i in range(len(layers)-1): arrow(d,115,layers[i][2],115,layers[i+1][2]+32,colors.HexColor("#546e7a"))
    label(d,420,290,"共享资源",10,colors.HexColor("#e65100"))
    for t,c,y in [("BlockCache(LRU)","#e65100",245),("WriteBufferMgr","#f57c00",200),("统一内存预算","#fb8c00",155)]:
        box(d,370,y,120,32,t,colors.HexColor(c),fs=7)
    arrow(d,430,245,430,232,colors.HexColor("#ff9800")); arrow(d,430,200,430,187,colors.HexColor("#ff9800"))
    label(d,250,110,"管理的Options类型",10,colors.HexColor("#455a64"))
    for t,c,x in [("DBOptions","#37474f",20),("ColumnFamily\nOptions","#455a64",130),("WriteOptions","#546e7a",250),("ReadOptions","#607d8b",370)]:
        box(d,x,55,110,35,t,colors.HexColor(c),fs=7)
    label(d,250,35,"所有Options在Backend关闭时由ResourceContainer统一释放,防止JNI内存泄漏",7,colors.HexColor("#9e9e9e"))
    return d

# === 构建PDF ===
def build():
    out=os.path.join(os.path.dirname(os.path.abspath(__file__)),"Flink_State_Backends_源码分析.pdf")
    doc=SimpleDocTemplate(out,pagesize=A4,rightMargin=2*cm,leftMargin=2*cm,topMargin=2.5*cm,bottomMargin=2*cm)
    st=mkstyles(); story=[]

    # 封面
    story+=[Spacer(1,80),Paragraph("Flink State Backends 模块",st["T"]),Paragraph("核心流程与设计说明",st["T"]),
            Spacer(1,20),HRFlowable(width="60%",thickness=2,color=colors.HexColor("#1565c0")),Spacer(1,20),
            Paragraph("基于 Apache Flink 1.15.4 Release 分支源码分析",st["ST"]),
            Paragraph("flink-state-backends 5 子模块完整解读",st["ST"]),Spacer(1,30),
            Paragraph("涵盖: flink-statebackend-rocksdb / forst / changelog / heap-spillable / common",st["ST"]),
            Paragraph("模块路径: flink-state-backends/",st["ST"]),PageBreak()]

    # 目录
    story+=[Paragraph("目录",st["H1"]),Spacer(1,10)]
    for t in ["一、模块概览与架构设计","二、RocksDB State Backend 核心类设计","三、ForSt State Backend 核心类设计",
              "四、Changelog State Backend 核心类设计","五、Heap-Spillable 与 Common 子模块",
              "六、Checkpoint / Snapshot 核心流程","七、恢复操作选择流程","八、ForSt 异步状态执行流程",
              "九、Changelog 物化循环流程","十、CopyOnWrite SkipList 工作原理",
              "十一、资源管理与配置体系","十二、关键设计模式与架构思想"]:
        story+=[Paragraph(t,st["B"]),Spacer(1,3)]
    story+=[PageBreak()]

    # 一、模块概览
    story+=[Paragraph("一、模块概览与架构设计",st["H1"]),HRFlowable(width="100%",thickness=1,color=colors.HexColor("#e0e0e0"))]
    story+=[Paragraph("1.1 模块定位",st["H2"])]
    story+=[Paragraph("flink-state-backends 是 Apache Flink 的状态存储核心模块，提供了多种 KeyedStateBackend 实现方案。该模块决定了 Flink 有状态算子的状态如何存储、快照和恢复。包含 5 个子模块，覆盖了从嵌入式本地存储(RocksDB)到新一代异步存储(ForSt)、增量日志(Changelog)、可溢出堆存储(Heap-Spillable)以及公共物化接口(Common)的完整方案。",st["B"])]
    story+=[Paragraph("1.2 子模块一览",st["H2"])]
    story+=[mktable(["子模块","核心类数","定位与特点"],
        [["flink-statebackend-rocksdb","79","基于RocksDB JNI嵌入式状态存储,支持增量Checkpoint"],
         ["flink-statebackend-forst","108","下一代状态存储,支持异步IO/远程存储/State API v2"],
         ["flink-statebackend-changelog","38","装饰器模式,记录状态变更日志,定期物化"],
         ["flink-statebackend-heap-spillable","13","基于SkipList堆外内存状态存储,支持COW快照"],
         ["flink-statebackend-common","2","公共接口:MaterializationTarget/Metadata"]],[140,55,255])]
    story+=[Spacer(1,10),Paragraph("1.3 架构总览",st["H2"]),fig_arch(),Paragraph("图1-1: Flink State Backends 架构总览",st["Cap"]),PageBreak()]

    # 二、RocksDB
    story+=[Paragraph("二、RocksDB State Backend 核心类设计",st["H1"]),HRFlowable(width="100%",thickness=1,color=colors.HexColor("#e0e0e0"))]
    story+=[Paragraph("2.1 EmbeddedRocksDBStateBackend (约1042行)",st["H2"])]
    story+=[Paragraph("RocksDB State Backend 的工厂类和配置入口，实现 ConfigurableStateBackend 接口。负责创建 RocksDBKeyedStateBackend 实例，管理 JNI 库加载、本地目录初始化和共享内存资源分配。",st["B"])]
    for r in ["createKeyedStateBackend(): 核心工厂方法，编排整个初始化流程: JNI加载->目录初始化->资源容器->Builder构建",
              "JNI库加载: 带重试的ensureRocksDBIsLoaded()，支持自定义native库路径",
              "lazyInitializeForJob(): 惰性初始化本地目录，支持多目录轮转(instanceRocksDBPath数组)",
              "共享内存管理: allocateSharedCachesIfConfigured()创建WriteBufferManager+BlockCache",
              "PredefinedOptions: 预定义模板(DEFAULT/SPINNING_DISK_OPTIMIZED/FLASH_SSD_OPTIMIZED等)",
              "RocksDBOptionsFactory: 用户自定义Options工厂，提供最高优先级配置覆盖"]:
        story+=[Paragraph(f"  \u2022 {r}",st["BL"])]

    story+=[Paragraph("2.2 RocksDBKeyedStateBackend (约1152行)",st["H2"])]
    story+=[Paragraph("实际的Keyed State Backend实现，将状态存储在嵌入式RocksDB实例中。每个状态注册为一个RocksDB Column Family。",st["B"])]
    for r in ["STATE_CREATE_FACTORIES: 静态Map映射StateDescriptor类型到对应创建工厂(策略模式)",
              "snapshot(): 委托给当前SnapshotStrategy(Full或Incremental)执行快照",
              "createOrUpdateInternalState(): 注册新状态或更新已有状态的Column Family，处理TTL",
              "RocksDBWriteBatchWrapper: 批量写入优化，积累到阈值再flush，减少JNI调用",
              "RocksDbTtlCompactFiltersManager: TTL过期状态通过RocksDB CompactFilter清理",
              "dispose(): 关闭所有Column Family Handle、释放RocksDB实例和原生资源"]:
        story+=[Paragraph(f"  \u2022 {r}",st["BL"])]

    story+=[Paragraph("2.3 RocksDBKeyedStateBackendBuilder (约724行)",st["H2"])]
    story+=[Paragraph("使用Builder模式组装RocksDBKeyedStateBackend，封装恢复和初始化逻辑。",st["B"])]
    for r in ["build(): 主流程—prepareDirectories->选择恢复操作->执行恢复->初始化快照策略->构建优先级队列工厂",
              "getRocksDBRestoreOperation(): 根据StateHandle类型选择None/Incremental/Full/HeapTimersFull",
              "恢复操作执行后获得RocksDBRestoreResult: 包含db实例、columnFamilyHandles、nativeMetricMonitor"]:
        story+=[Paragraph(f"  \u2022 {r}",st["BL"])]

    story+=[Paragraph("2.4 RocksDBResourceContainer (约516行)",st["H2"])]
    story+=[Paragraph("管理RocksDB原生Options对象完整生命周期，实现配置分层覆盖。确保JNI分配的原生内存在关闭时正确释放。",st["B"])]
    for r in ["配置分层优先级: 用户Factory>Flink ConfigurableOptions>PredefinedOptions>RocksDB默认",
              "共享BlockCache和WriteBufferManager: 所有ColumnFamily共享同一组缓存",
              "close(): 逐一释放所有已创建的Options对象，防止JNI内存泄漏"]:
        story+=[Paragraph(f"  \u2022 {r}",st["BL"])]

    story+=[Paragraph("2.5 快照与恢复类",st["H2"])]
    story+=[mktable(["类名","核心职责"],
        [["RocksNativeFullSnapshotStrategy","全量快照: getSnapshot()->遍历CF Iterator->写入CheckpointStream"],
         ["RocksIncrementalSnapshotStrategy","增量快照: flush->getLiveFiles->diff->上传新增SST"],
         ["RocksDBNoneRestoreOperation","创建空DB, 初始化默认ColumnFamilies"],
         ["RocksDBFullRestoreOperation","反序列化全量快照数据, 逐KV写入新DB"],
         ["RocksDBIncrementalRestoreOperation","下载SST+manifest, IngestExternalFile恢复"],
         ["RocksDBHeapTimersFullRestoreOperation","全量恢复+Timer状态分离到Heap"],
         ["RocksDBManualCompactionManager","手动Compaction管理, 优化SST文件布局"],
         ["RocksDbTtlCompactFiltersManager","TTL CompactFilter, 通过RocksDB原生过滤器清理过期状态"]],[160,290])]
    story+=[PageBreak()]

    # 三、ForSt
    story+=[Paragraph("三、ForSt State Backend 核心类设计",st["H1"]),HRFlowable(width="100%",thickness=1,color=colors.HexColor("#e0e0e0"))]
    story+=[Paragraph("3.1 ForStStateBackend (约997行)",st["H2"])]
    story+=[Paragraph("下一代状态存储后端(@Experimental)。基于ForSt(RocksDB增强分支)，支持远程存储溢出和异步状态操作(State API v2)。同时支持同步和异步两种模式。",st["B"])]
    for r in ["createAsyncKeyedStateBackend(): 创建ForStKeyedStateBackend, 返回AsyncKeyedStateBackend接口",
              "远程存储: ForStFlinkFileSystem将checkpoint-dir作为远程溢出目标",
              "ForStPathContainer: 管理本地数据路径和远程路径映射",
              "异步JNI加载: 使用executor在后台线程加载ForSt原生库"]:
        story+=[Paragraph(f"  \u2022 {r}",st["BL"])]

    story+=[Paragraph("3.2 ForStKeyedStateBackend (约666行)",st["H2"])]
    story+=[Paragraph("实现AsyncKeyedStateBackend&lt;K&gt;接口, 是State API v2核心实现。支持异步批量状态操作。",st["B"])]
    for r in ["createStateInternal(): 按状态类型(VALUE/LIST/MAP/REDUCING/AGGREGATING)创建实现",
              "createStateExecutor(): 创建ForStStateExecutor, 配置coordinator/read/write三个线程池",
              "dispose(): synchronized(lock)保护managedStateExecutors, 确保线程安全关闭"]:
        story+=[Paragraph(f"  \u2022 {r}",st["BL"])]

    story+=[Paragraph("3.3 ForStStateExecutor (约343行)",st["H2"])]
    story+=[Paragraph("异步批量状态请求执行器。Coordinator线程+Read线程池+Write线程池三层分离模型。",st["B"])]
    for r in ["executeBatchRequests(): 分类为Get/Iterate/Put三种请求",
              "ForStGeneralMultiGetOperation: 批量Get, 利用RocksDB multiGet API",
              "ForStWriteBatchOperation: 批量写入, WriteBatch原子写入",
              "callbackRunner: IO完成后回调Coordinator线程"]:
        story+=[Paragraph(f"  \u2022 {r}",st["BL"])]

    story+=[Paragraph("3.4 其他关键类",st["H2"])]
    story+=[mktable(["类名","核心职责"],
        [["ForStResourceContainer","管理ForSt Options, 配置分层逻辑与RocksDB版一致"],
         ["ForStOperationUtils","ForSt操作工具: openDB()/createColumnFamily()"],
         ["ForStFlinkFileSystem","桥接Flink FileSystem到ForSt Env, 支持远程存储"],
         ["ForStIncrementalSnapshotStrategy","ForSt专属增量快照策略, 支持远程SST管理"],
         ["ForStDBTtlCompactFiltersManager","ForSt专属TTL CompactFilter管理"],
         ["ForStKeyedStateBackendBuilder","构建ForStKeyedStateBackend, 含异步IO线程池配置"]],[160,290])]
    story+=[PageBreak()]

    # 四、Changelog
    story+=[Paragraph("四、Changelog State Backend 核心类设计",st["H1"]),HRFlowable(width="100%",thickness=1,color=colors.HexColor("#e0e0e0"))]
    story+=[Paragraph("4.1 ChangelogStateBackend (约143行)",st["H2"])]
    story+=[Paragraph("工厂类。使用装饰器模式在底层Backend(RocksDB/ForSt)之上添加Changelog日志层, 实现更小粒度Checkpoint。",st["B"])]
    for r in ["restore(): 编排恢复—ChangelogBackendRestoreOperation->创建changelogWriter->启动周期物化",
              "DelegatedStateBackend: 实际存储委托给配置的底层Backend",
              "StateChangelogWriter: 记录每个状态变更到Changelog"]:
        story+=[Paragraph(f"  \u2022 {r}",st["BL"])]

    story+=[Paragraph("4.2 ChangelogKeyedStateBackend (约1201行)",st["H2"])]
    story+=[Paragraph("装饰器类, 继承AbstractKeyedStateBackend并实现MaterializationTarget。拦截所有状态操作, 委托底层Backend同时记录变更。",st["B"])]
    for r in ["snapshot(): stateChangelogWriter.persist(from,to)持久化Changelog范围, 构建ChangelogStateBackendHandle",
              "ChangelogSnapshotState: 内部类, 封装已物化快照+未物化Changelog组合",
              "initMaterialization(): 触发底层Backend的snapshot作为物化操作",
              "状态变更拦截: 每个create/update/remove通过ChangelogState包装器记录StateChange"]:
        story+=[Paragraph(f"  \u2022 {r}",st["BL"])]

    story+=[Paragraph("4.3 PeriodicMaterializationManager (约427行)",st["H2"])]
    story+=[Paragraph("周期性物化管理器, 调度定期将Changelog物化为底层Backend完整快照, 截断已物化Changelog段。",st["B"])]
    for r in ["triggerMaterialization(): 在mailbox executor中触发物化流程",
              "随机初始延迟: initialDelay×[0.75,1.25]随机因子, 避免惊群效应",
              "失败容忍: allowedNumberOfFailures配置, 超过后Task失败",
              "asyncMaterializationPhase(): 异步执行checkpoint数据上传"]:
        story+=[Paragraph(f"  \u2022 {r}",st["BL"])]
    story+=[PageBreak()]

    # 五、Heap-Spillable + Common
    story+=[Paragraph("五、Heap-Spillable 与 Common 子模块",st["H1"]),HRFlowable(width="100%",thickness=1,color=colors.HexColor("#e0e0e0"))]
    story+=[Paragraph("5.1 CopyOnWriteSkipListStateMap (约1589行)",st["H2"])]
    story+=[Paragraph("基于SkipList的状态存储, 使用堆外内存和Copy-on-Write机制支持并发快照。",st["B"])]
    for r in ["SkipList多层索引: 随机层高, O(log n)查找/插入/删除",
              "Copy-on-Write: node.version &lt; highestSnapshotVersion时触发, 保留旧值供快照读取",
              "逻辑删除vs物理删除: remove先标记, 快照完成后再物理删除回收空间",
              "Off-Heap内存: Allocator+Chunk(1MB)+SpaceAllocator, 避免GC压力",
              "stateSnapshot(): 递增版本号, 注册快照版本, 返回CopyOnWriteSkipListStateMapSnapshot"]:
        story+=[Paragraph(f"  \u2022 {r}",st["BL"])]
    story+=[Paragraph("5.2 Common子模块",st["H2"])]
    story+=[Paragraph("最小子模块(2个核心类), 为Changelog物化提供公共抽象。",st["B"])]
    for r in ["MaterializationTarget: 物化目标接口, 被ChangelogKeyedStateBackend实现",
              "ChangelogMaterializationMetadata: 封装物化操作元数据(ID/起始序列号/目标序列号)"]:
        story+=[Paragraph(f"  \u2022 {r}",st["BL"])]
    story+=[PageBreak()]

    # 六、Checkpoint流程
    story+=[Paragraph("六、Checkpoint / Snapshot 核心流程",st["H1"]),HRFlowable(width="100%",thickness=1,color=colors.HexColor("#e0e0e0"))]
    story+=[Paragraph("Checkpoint是Flink容错核心。State Backend负责将算子状态持久化到分布式存储。RocksDB支持全量和增量两种快照策略。",st["B"])]
    story+=[fig_checkpoint(),Paragraph("图6-1: RocksDB Checkpoint流程(全量 vs 增量)",st["Cap"])]
    for d in ["全量快照: 使用RocksDB原生Snapshot获取一致性视图, 遍历所有ColumnFamily Iterator读取全部KV",
              "增量快照: flush memtable后获取LiveFiles列表, 对比上次checkpoint计算新增SST文件, 仅上传新增SST",
              "异步执行: 同步阶段(获取Snapshot/LiveFiles)在主线程, 异步阶段(数据上传)在独立线程池",
              "KeyGroupRange分组: 数据按KeyGroup范围分区写入, 支持rescale时细粒度状态重分配"]:
        story+=[Paragraph(f"  \u2022 {d}",st["BL"])]
    story+=[PageBreak()]

    # 七、恢复流程
    story+=[Paragraph("七、恢复操作选择流程",st["H1"]),HRFlowable(width="100%",thickness=1,color=colors.HexColor("#e0e0e0"))]
    story+=[Paragraph("RocksDB Backend恢复时根据StateHandle类型选择合适的恢复操作。",st["B"])]
    story+=[fig_restore(),Paragraph("图7-1: RocksDB恢复操作选择流程",st["Cap"])]
    for d in ["NoneRestore: 全新启动, 创建空RocksDB实例",
              "FullRestore: 反序列化CheckpointStream中KV数据, 逐条写入新RocksDB",
              "IncrementalRestore: 下载SST文件和MANIFEST, 使用IngestExternalFile恢复",
              "HeapTimersFullRestore: 全量恢复变种, Timer状态分离到Heap",
              "Changelog恢复: 先恢复物化快照, 再回放Changelog增量日志"]:
        story+=[Paragraph(f"  \u2022 {d}",st["BL"])]
    story+=[PageBreak()]

    # 八、ForSt异步
    story+=[Paragraph("八、ForSt 异步状态执行流程",st["H1"]),HRFlowable(width="100%",thickness=1,color=colors.HexColor("#e0e0e0"))]
    story+=[Paragraph("ForSt引入异步批量状态执行模型(State API v2)。ForStStateExecutor将请求分类后分发到独立Read/Write IO线程池, 实现读写分离。",st["B"])]
    story+=[fig_forst(),Paragraph("图8-1: ForSt异步状态执行流程",st["Cap"])]
    for d in ["读写线程分离: Read线程处理Get/Iterate, Write线程处理Put, 避免相互阻塞",
              "批量MultiGet: 多个Get请求合并为RocksDB multiGet API调用",
              "WriteBatch原子写入: 多个Put合并为WriteBatch, 减少WAL写入次数",
              "Coordinator线程负责请求分类和结果收集, 不执行IO操作"]:
        story+=[Paragraph(f"  \u2022 {d}",st["BL"])]
    story+=[PageBreak()]

    # 九、Changelog物化
    story+=[Paragraph("九、Changelog 物化循环流程",st["H1"]),HRFlowable(width="100%",thickness=1,color=colors.HexColor("#e0e0e0"))]
    story+=[Paragraph("Changelog通过定期物化(Periodic Materialization)控制日志增长。物化将Changelog持久化为底层Backend完整快照, 截断已物化Changelog段。",st["B"])]
    story+=[fig_changelog(),Paragraph("图9-1: Changelog物化循环流程",st["Cap"])]
    for d in ["Checkpoint时持久化Changelog范围[lastMaterialized,current], 物化后移动lastMaterialized",
              "惊群效应防护: 初始延迟×随机因子[0.75,1.25]",
              "Mailbox线程模型: triggerMaterialization在mailbox线程中执行, 确保线程安全",
              "失败容忍: allowedNumberOfFailures, 超过后Task失败"]:
        story+=[Paragraph(f"  \u2022 {d}",st["BL"])]
    story+=[PageBreak()]

    # 十、SkipList
    story+=[Paragraph("十、CopyOnWrite SkipList 工作原理",st["H1"]),HRFlowable(width="100%",thickness=1,color=colors.HexColor("#e0e0e0"))]
    story+=[Paragraph("CopyOnWriteSkipListStateMap结合SkipList有序索引和COW并发快照支持, 使用堆外内存减少GC开销。",st["B"])]
    story+=[fig_skiplist(),Paragraph("图10-1: CopyOnWrite SkipList工作原理",st["Cap"])]
    for d in ["Off-Heap存储: 所有Key/Value在堆外内存中, 通过long地址引用",
              "版本链: Value节点有nextVersion指针, 快照读取时沿版本链查找匹配版本",
              "COW条件: 仅node.version &lt; highestSnapshotVersion时触发, 最小化复制开销",
              "空间回收: pruneTimestamp/pruneSpace在快照完成后清理旧版本, 回收堆外内存"]:
        story+=[Paragraph(f"  \u2022 {d}",st["BL"])]
    story+=[PageBreak()]

    # 十一、资源管理
    story+=[Paragraph("十一、资源管理与配置体系",st["H1"]),HRFlowable(width="100%",thickness=1,color=colors.HexColor("#e0e0e0"))]
    story+=[Paragraph("RocksDB和ForSt的配置体系采用分层覆盖设计, 通过ResourceContainer统一管理Options生命周期。",st["B"])]
    story+=[fig_resource(),Paragraph("图11-1: RocksDB资源容器配置层次",st["Cap"])]
    story+=[mktable(["配置分类","关键配置项","说明"],
        [["内存管理","state.backend.rocksdb.memory.managed","共享内存分配比例"],
         ["写入优化","writebuffer.size/count","Write Buffer参数"],
         ["Compaction","compaction.style/level.target-file-size","Compaction策略"],
         ["读取优化","block.blocksize/cache-size/bloom-filter","Block Cache和布隆过滤器"],
         ["增量CP","state.backend.incremental","增量Checkpoint配置"],
         ["TTL","ttl.compaction.filter.enabled","TTL CompactFilter开关"],
         ["Timer","timer-service.factory","Timer存储(ROCKSDB/HEAP)"],
         ["Changelog","periodic-materialization-interval","物化间隔和失败容忍"],
         ["ForSt IO","read/write.io-thread-num","ForSt读写线程数"]],[80,180,190])]
    story+=[PageBreak()]

    # 十二、设计模式
    story+=[Paragraph("十二、关键设计模式与架构思想",st["H1"]),HRFlowable(width="100%",thickness=1,color=colors.HexColor("#e0e0e0"))]
    patterns=[
        ("Builder 模式","RocksDBKeyedStateBackendBuilder/ForStKeyedStateBackendBuilder — 封装复杂的恢复、快照策略初始化和资源管理逻辑, 通过build()方法一步完成Backend构建"),
        ("装饰器模式","ChangelogKeyedStateBackend包装底层RocksDB/ForSt Backend, 在不修改底层实现的情况下添加Changelog记录功能"),
        ("策略模式","SnapshotStrategy接口(Full/Incremental), RestoreOperation接口(None/Full/Incremental/HeapTimers) — 运行时根据配置和状态选择具体策略"),
        ("工厂模式","STATE_CREATE_FACTORIES映射StateDescriptor到创建工厂; StateBackend.createKeyedStateBackend()工厂方法"),
        ("Copy-on-Write","CopyOnWriteSkipListStateMap在有活跃快照时创建新版本节点, 旧版本通过版本链保留, 实现无锁并发快照"),
        ("资源容器","RocksDBResourceContainer/ForStResourceContainer — 统一管理JNI原生Options生命周期, 防止内存泄漏"),
        ("配置分层覆盖","四层配置优先级: 用户Factory>ConfigurableOptions>PredefinedOptions>Default, 灵活性和安全性兼顾"),
        ("读写分离","ForStStateExecutor将IO操作分为Read线程池和Write线程池, 避免读写相互阻塞, 提升并发吞吐"),
        ("定期物化","PeriodicMaterializationManager定时将Changelog物化为底层完整快照, 随机延迟避免惊群, 失败容忍机制保证鲁棒性"),
        ("异步CompletableFuture编排","恢复操作、快照上传、物化等长耗时操作均使用CompletableFuture异步执行, 不阻塞主处理线程"),
    ]
    for name,desc in patterns:
        story+=[Paragraph(f"<b>{name}</b>",st["H3"]),Paragraph(desc,st["BI"])]

    story+=[Spacer(1,30),HRFlowable(width="100%",thickness=2,color=colors.HexColor("#1565c0"))]
    story+=[Paragraph("文档结束 — 基于 Flink 1.15.4 flink-state-backends 模块全部源码分析生成",st["Cap"])]

    doc.build(story)
    print(f"PDF generated: {out}")
    return out

if __name__=="__main__":
    build()
