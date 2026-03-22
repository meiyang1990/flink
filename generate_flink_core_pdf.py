#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Flink Core 模块源码分析 - PDF 生成脚本"""

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

# ============================================================
# 图1: flink-core 模块架构总览
# ============================================================
def fig_arch():
    d=Drawing(500,470)
    d.add(Rect(0,0,500,470,fillColor=colors.HexColor("#fafafa"),strokeColor=colors.HexColor("#e0e0e0"),strokeWidth=0.5))
    label(d,250,450,"flink-core 模块架构总览",13,colors.HexColor("#1565c0"))

    # 层1: API层
    box(d,30,400,440,35,"API 层  (api.common / api.connector / api.dag / api.java)",colors.HexColor("#0d47a1"),fs=10)
    # 层2: 配置系统
    box(d,30,340,210,45,"配置系统\n(configuration)",colors.HexColor("#e65100"),fs=9)
    box(d,260,340,210,45,"类型系统\n(typeinfo + typeutils)",colors.HexColor("#2e7d32"),fs=9)
    # 层3: 核心基础设施
    box(d,30,270,130,50,"文件系统\n(core.fs)",colors.HexColor("#6a1b9a"),fs=9)
    box(d,175,270,130,50,"内存管理\n(core.memory)",colors.HexColor("#00695c"),fs=9)
    box(d,320,270,150,50,"执行引擎\n(core.execution)",colors.HexColor("#c62828"),fs=9)
    # 层4: 基础设施
    box(d,30,200,130,50,"IO / 序列化\n(core.io)",colors.HexColor("#4e342e"),fs=9)
    box(d,175,200,130,50,"插件系统\n(core.plugin)",colors.HexColor("#1b5e20"),fs=9)
    box(d,320,200,150,50,"安全 / 类加载\n(core.security)",colors.HexColor("#0d47a1"),fs=9)
    # 层5: 工具层
    box(d,30,140,440,40,"工具类层  (util / types / management)",colors.HexColor("#37474f"),fs=10)

    # 箭头
    arrow(d,250,400,135,385,colors.HexColor("#90caf9"),0.8)
    arrow(d,250,400,365,385,colors.HexColor("#90caf9"),0.8)
    for x in [95,240,395]: arrow(d,x,340,x,320,colors.HexColor("#b0bec5"),0.8)
    for x in [95,240,395]: arrow(d,x,270,x,250,colors.HexColor("#b0bec5"),0.8)
    for x in [95,240,395]: arrow(d,x,200,x,180,colors.HexColor("#b0bec5"),0.8)

    # 右侧标签
    label(d,250,110,"7个顶层包 | 500+ Java文件 | Flink全模块共同依赖",8,colors.HexColor("#757575"))
    return d

# ============================================================
# 图2: 配置系统加载流程
# ============================================================
def fig_config_loading():
    d=Drawing(500,380)
    d.add(Rect(0,0,500,380,fillColor=colors.HexColor("#fafafa"),strokeColor=colors.HexColor("#e0e0e0"),strokeWidth=0.5))
    label(d,250,360,"配置系统加载流程",13,colors.HexColor("#1565c0"))

    steps=[
        ("GlobalConfiguration\n.loadConfiguration()",330,"#0d47a1"),
        ("读取 FLINK_CONF_DIR\n环境变量",290,"#e65100"),
        ("YamlParserUtils\n.loadYamlFile()",250,"#2e7d32"),
        ("flatten() 嵌套键\n展平为点分隔键",210,"#6a1b9a"),
        ("setValueInternal()\n写入 confData HashMap",170,"#00695c"),
        ("合并 dynamicProperties\n(命令行 -D 参数)",130,"#c62828"),
        ("返回 Configuration 对象",90,"#0d47a1"),
    ]
    for txt,y,c in steps:
        box(d,140,y,220,32,txt,colors.HexColor(c),fs=8)
    for i in range(len(steps)-1):
        arrow(d,250,steps[i][1],250,steps[i+1][1]+32,colors.HexColor("#90caf9"),0.8)

    # 右侧说明
    label(d,420,310,"config.yaml",8,colors.HexColor("#e65100"))
    label(d,420,270,"嵌套YAML + 平铺YAML",7,colors.HexColor("#455a64"))
    label(d,420,230,"a.b.c: value",7,colors.HexColor("#455a64"))
    label(d,420,150,"优先级: 动态参数 &gt; 配置文件",7,colors.HexColor("#c62828"))
    return d

# ============================================================
# 图3: 类型系统与序列化体系
# ============================================================
def fig_type_system():
    d=Drawing(500,420)
    d.add(Rect(0,0,500,420,fillColor=colors.HexColor("#fafafa"),strokeColor=colors.HexColor("#e0e0e0"),strokeWidth=0.5))
    label(d,250,400,"类型系统与序列化体系",13,colors.HexColor("#1565c0"))

    # TypeInformation 顶层
    box(d,160,350,180,32,"TypeInformation&lt;T&gt;\n(类型元数据核心抽象)",colors.HexColor("#0d47a1"),fs=8)

    # 子类型
    subs=[("BasicTypeInfo",20,"#e65100"),("TupleTypeInfo",130,"#2e7d32"),("PojoTypeInfo",240,"#6a1b9a"),("GenericTypeInfo",350,"#00695c")]
    for txt,x,c in subs:
        box(d,x,290,110,28,txt,colors.HexColor(c),fs=7)
        arrow(d,x+55,350,x+55,318,colors.HexColor("#b0bec5"),0.8)

    # TypeSerializer
    box(d,160,230,180,32,"TypeSerializer&lt;T&gt;\n(序列化/反序列化核心)",colors.HexColor("#c62828"),fs=8)
    arrow(d,250,290,250,262,colors.HexColor("#546e7a"),1.0)
    label(d,290,273,"createSerializer()",7,colors.HexColor("#c62828"))

    # 序列化器子类
    sers=[("IntSerializer",20,"#e65100"),("StringSerializer",130,"#2e7d32"),("PojoSerializer",240,"#6a1b9a"),("KryoSerializer",350,"#00695c")]
    for txt,x,c in sers:
        box(d,x,170,110,28,txt,colors.HexColor(c),fs=7)
        arrow(d,x+55,230,x+55,198,colors.HexColor("#b0bec5"),0.8)

    # TypeSerializerSnapshot
    box(d,160,110,180,32,"TypeSerializerSnapshot&lt;T&gt;\n(快照兼容性检查)",colors.HexColor("#4e342e"),fs=8)
    arrow(d,250,170,250,142,colors.HexColor("#546e7a"),1.0)
    label(d,310,153,"snapshotConfiguration()",7,colors.HexColor("#4e342e"))

    # 底部兼容性
    comps=[("COMPATIBLE_AS_IS",70,"#2e7d32"),("COMPATIBLE_AFTER\n_MIGRATION",220,"#e65100"),("INCOMPATIBLE",380,"#c62828")]
    for txt,x,c in comps:
        box(d,x-50,50,120,35,txt,colors.HexColor(c),fs=6)
        arrow(d,x+10,110,x+10,85,colors.HexColor("#b0bec5"),0.8)
    label(d,250,30,"resolveSchemaCompatibility()",7,colors.HexColor("#757575"))
    return d

# ============================================================
# 图4: 文件系统抽象与发现机制
# ============================================================
def fig_filesystem():
    d=Drawing(500,400)
    d.add(Rect(0,0,500,400,fillColor=colors.HexColor("#fafafa"),strokeColor=colors.HexColor("#e0e0e0"),strokeWidth=0.5))
    label(d,250,380,"文件系统抽象与发现机制",13,colors.HexColor("#1565c0"))

    # FileSystem.get(uri) 入口
    box(d,160,335,180,30,"FileSystem.get(URI)",colors.HexColor("#0d47a1"),fs=9)

    # 步骤
    box(d,30,280,140,30,"解析 URI scheme\n+ authority",colors.HexColor("#e65100"),fs=7)
    box(d,190,280,120,30,"查 CACHE\nFSKey缓存",colors.HexColor("#2e7d32"),fs=7)
    box(d,330,280,140,30,"FS_FACTORIES\nscheme工厂查找",colors.HexColor("#6a1b9a"),fs=7)

    arrow(d,250,335,100,310,colors.HexColor("#90caf9"),0.8)
    arrow(d,100,280,250,280,colors.HexColor("#90caf9"),0.8)
    arrow(d,310,280,330,280,colors.HexColor("#90caf9"),0.8)

    # 工厂分支
    box(d,30,210,120,35,"LocalFileSystem\nFactory",colors.HexColor("#4e342e"),fs=7)
    box(d,170,210,120,35,"ServiceLoader\n文件系统工厂",colors.HexColor("#1b5e20"),fs=7)
    box(d,320,210,150,35,"PluginManager\n插件文件系统工厂",colors.HexColor("#c62828"),fs=7)

    arrow(d,400,280,400,245,colors.HexColor("#b0bec5"),0.8)
    for x in [90,230,395]:
        arrow(d,x,210,x,190,colors.HexColor("#b0bec5"),0.8)

    # Fallback
    box(d,170,155,160,30,"FALLBACK_FACTORY\nHadoopFsFactory",colors.HexColor("#37474f"),fs=7)
    arrow(d,250,210,250,185,colors.HexColor("#90caf9"),0.8)

    # 底层实现
    impls=[("local://",40,"#4e342e"),("hdfs://",140,"#e65100"),("s3://",240,"#2e7d32"),("gs://",330,"#6a1b9a"),("oss://",420,"#00695c")]
    for txt,x,c in impls:
        box(d,x-10,90,80,28,txt,colors.HexColor(c),fs=8)

    # Safety Net
    box(d,160,35,180,30,"FileSystemSafetyNet\n(流泄漏保护)",colors.HexColor("#0d47a1"),fs=8)
    arrow(d,250,90,250,65,colors.HexColor("#546e7a"),0.8)
    return d

# ============================================================
# 图5: MemorySegment 内存管理模型
# ============================================================
def fig_memory():
    d=Drawing(500,380)
    d.add(Rect(0,0,500,380,fillColor=colors.HexColor("#fafafa"),strokeColor=colors.HexColor("#e0e0e0"),strokeWidth=0.5))
    label(d,250,360,"MemorySegment 内存管理模型",13,colors.HexColor("#1565c0"))

    # 中心: MemorySegment
    box(d,150,280,200,40,"MemorySegment\n(统一内存抽象)",colors.HexColor("#0d47a1"),fs=10)

    # 三种模式
    box(d,20,200,130,45,"On-Heap\nbyte[] heapMemory\nUNSAFE访问",colors.HexColor("#2e7d32"),fs=7)
    box(d,185,200,130,45,"Off-Heap Direct\nByteBuffer\noffHeapBuffer",colors.HexColor("#e65100"),fs=7)
    box(d,350,200,130,45,"Off-Heap Unsafe\naddress直接寻址\ncleaner回调",colors.HexColor("#c62828"),fs=7)

    arrow(d,200,280,85,245,colors.HexColor("#90caf9"),0.8)
    arrow(d,250,280,250,245,colors.HexColor("#90caf9"),0.8)
    arrow(d,300,280,415,245,colors.HexColor("#90caf9"),0.8)

    # 关键操作
    ops=[("get/put\n随机读写",30,"#4e342e"),("getInt/getLong\nUnsafe原语",130,"#1b5e20"),("copyTo\n段间拷贝",230,"#6a1b9a"),("compare\n字节比较",330,"#00695c"),("wrap\nByteBuffer视图",430,"#0d47a1")]
    for txt,x,c in ops:
        box(d,x-25,120,90,38,txt,colors.HexColor(c),fs=6)

    # 生命周期
    box(d,100,50,130,35,"MemorySegmentFactory\n.allocate...()",colors.HexColor("#37474f"),fs=7)
    box(d,280,50,130,35,"free()\naddress=limit+1\nGC回收",colors.HexColor("#c62828"),fs=7)
    arrow(d,230,50,280,50,colors.HexColor("#546e7a"),0.8)
    label(d,255,42,"生命周期",7,colors.HexColor("#757575"))
    return d

# ============================================================
# 图6: Source 连接器架构 (FLIP-27)
# ============================================================
def fig_source_connector():
    d=Drawing(500,380)
    d.add(Rect(0,0,500,380,fillColor=colors.HexColor("#fafafa"),strokeColor=colors.HexColor("#e0e0e0"),strokeWidth=0.5))
    label(d,250,360,"Source 连接器架构 (FLIP-27)",13,colors.HexColor("#1565c0"))

    # Source 接口
    box(d,170,310,160,30,"Source&lt;T, SplitT, EnumChkT&gt;\n(工厂接口)",colors.HexColor("#0d47a1"),fs=8)

    # 两个核心组件
    box(d,40,240,170,35,"SplitEnumerator\n(JobManager端)\n分片发现与分配",colors.HexColor("#e65100"),fs=7)
    box(d,290,240,170,35,"SourceReader\n(TaskManager端)\n数据读取与输出",colors.HexColor("#2e7d32"),fs=7)

    arrow(d,210,310,125,275,colors.HexColor("#90caf9"),0.8)
    arrow(d,290,310,375,275,colors.HexColor("#90caf9"),0.8)
    label(d,140,290,"createEnumerator()",6,colors.HexColor("#e65100"))
    label(d,380,290,"createReader()",6,colors.HexColor("#2e7d32"))

    # 通信
    arrow(d,210,255,290,255,colors.HexColor("#546e7a"),1.0)
    arrow(d,290,248,210,248,colors.HexColor("#546e7a"),1.0)
    label(d,250,260,"addSplits()",6,colors.HexColor("#c62828"))
    label(d,250,242,"sendEvent()",6,colors.HexColor("#c62828"))

    # 序列化器
    box(d,40,170,170,30,"SimpleVersionedSerializer\n&lt;SplitT&gt;",colors.HexColor("#6a1b9a"),fs=7)
    box(d,290,170,170,30,"SimpleVersionedSerializer\n&lt;EnumChkT&gt;",colors.HexColor("#6a1b9a"),fs=7)
    arrow(d,125,240,125,200,colors.HexColor("#b0bec5"),0.8)
    arrow(d,375,240,375,200,colors.HexColor("#b0bec5"),0.8)

    # Boundedness
    box(d,180,110,140,30,"Boundedness\nBOUNDED | CONTINUOUS",colors.HexColor("#4e342e"),fs=7)
    arrow(d,250,170,250,140,colors.HexColor("#b0bec5"),0.8)

    # Checkpoint
    box(d,60,55,160,30,"SplitEnumerator\n.snapshotState(checkpointId)",colors.HexColor("#c62828"),fs=6)
    box(d,280,55,160,30,"SourceReader\n.snapshotState(checkpointId)",colors.HexColor("#c62828"),fs=6)
    arrow(d,125,170,140,85,colors.HexColor("#b0bec5"),0.8)
    arrow(d,375,170,360,85,colors.HexColor("#b0bec5"),0.8)
    return d

# ============================================================
# 图7: 作业执行流程 (Pipeline -> JobClient)
# ============================================================
def fig_execution():
    d=Drawing(500,350)
    d.add(Rect(0,0,500,350,fillColor=colors.HexColor("#fafafa"),strokeColor=colors.HexColor("#e0e0e0"),strokeWidth=0.5))
    label(d,250,330,"作业执行流程 (Pipeline -&gt; JobClient)",13,colors.HexColor("#1565c0"))

    steps=[
        ("StreamExecutionEnvironment\n.execute()",290,"#0d47a1"),
        ("Pipeline (StreamGraph)\n构建DAG",250,"#e65100"),
        ("PipelineExecutorFactory\n.getExecutor()",210,"#2e7d32"),
        ("PipelineExecutor\n.execute(pipeline, config, classLoader)",170,"#6a1b9a"),
        ("CompletableFuture&lt;JobClient&gt;\n异步返回",130,"#00695c"),
        ("JobClient\n(getJobStatus / cancel / triggerSavepoint)",90,"#c62828"),
    ]
    for txt,y,c in steps:
        box(d,110,y,280,32,txt,colors.HexColor(c),fs=8)
    for i in range(len(steps)-1):
        arrow(d,250,steps[i][1],250,steps[i+1][1]+32,colors.HexColor("#90caf9"),0.8)

    # 左侧标注
    label(d,65,280,"用户API",7,colors.HexColor("#455a64"))
    label(d,65,240,"图转换",7,colors.HexColor("#455a64"))
    label(d,65,200,"SPI发现",7,colors.HexColor("#455a64"))
    label(d,65,160,"提交执行",7,colors.HexColor("#455a64"))
    label(d,65,120,"异步Future",7,colors.HexColor("#455a64"))
    label(d,65,80,"交互控制",7,colors.HexColor("#455a64"))
    return d

# ============================================================
# 正文内容构建
# ============================================================
def build_pdf():
    st=mkstyles()
    story=[]

    # ========== 封面 ==========
    story+=[Spacer(1,80)]
    story+=[Paragraph("Flink Core 模块源码分析",st["T"])]
    story+=[Paragraph("flink-core 核心流程与设计说明",st["ST"])]
    story+=[Spacer(1,20)]
    story+=[HRFlowable(width="60%",thickness=2,color=colors.HexColor("#1565c0"),spaceAfter=20)]
    story+=[Paragraph("基于 Flink release-1.15.4 (version 2.3-SNAPSHOT)",st["ST"])]
    story+=[Paragraph("500+ Java 源文件 | 7个顶层包 | Flink 全模块共同依赖",st["B"])]
    story+=[PageBreak()]

    # ========== 目录 ==========
    story+=[Paragraph("目录",st["H1"])]
    toc_items=[
        "一、模块架构总览","二、配置系统 (Configuration)","三、类型系统与序列化体系",
        "四、文件系统抽象 (FileSystem)","五、内存管理 (MemorySegment)",
        "六、Source 连接器架构 (FLIP-27)","七、作业执行流程",
        "八、状态描述符 (StateDescriptor)","九、IO 与序列化框架",
        "十、插件系统 (Plugin)","十一、工具类体系","十二、关键设计模式总结",
    ]
    for it in toc_items: story+=[Paragraph(it,st["BI"])]
    story+=[PageBreak()]

    # ========== 一、模块架构总览 ==========
    story+=[Paragraph("一、模块架构总览",st["H1"])]
    story+=[Paragraph("flink-core 是 Flink 框架的基础核心模块，几乎所有其他 Flink 模块都依赖于它。该模块包含配置系统、类型系统、文件系统抽象、内存管理、IO框架、插件机制、执行引擎接口和大量通用工具类。",st["B"])]
    story+=[fig_arch()]
    story+=[Paragraph("图1: flink-core 模块五层架构总览",st["Cap"])]

    story+=[Paragraph("模块结构统计",st["H2"])]
    story+=[mktable(
        [Paragraph(h,st["B"]) for h in ["顶层包","核心职责","文件数(约)"]],
        [[Paragraph(c,st["B"]) for c in r] for r in [
            ["api/","用户编程接口: 函数、类型信息、状态、连接器、DAG","120+"],
            ["configuration/","配置系统: ConfigOption、Configuration、GlobalConfiguration","60+"],
            ["core/","基础设施: 文件系统、内存、IO、插件、执行引擎、安全","120+"],
            ["types/","数据类型: Value 类型、Row、Either、Record","30"],
            ["util/","通用工具: 集合、并发、异常、文件、网络、函数式","85+"],
            ["management/","JMX 管理接口","3"],
            ["streaming/","流处理 API 桥接","5"],
        ]],
        [80,200,60]
    )]
    story+=[PageBreak()]

    # ========== 二、配置系统 ==========
    story+=[Paragraph("二、配置系统 (Configuration)",st["H1"])]
    story+=[Paragraph("Flink的配置系统由三大核心类协作完成: ConfigOption定义配置项元数据, Configuration存储键值对, GlobalConfiguration从文件系统加载配置。",st["B"])]

    story+=[fig_config_loading()]
    story+=[Paragraph("图2: 配置系统加载流程 (GlobalConfiguration -> Configuration)",st["Cap"])]

    story+=[Paragraph("2.1 Configuration 类",st["H2"])]
    story+=[Paragraph("Configuration 是Flink的核心键值配置容器(694行)。内部使用 HashMap&lt;String, Object&gt; 存储配置数据, 支持多种类型(String/Int/Long/Boolean/Float/Double/byte[])的序列化和反序列化。",st["B"])]
    story+=[Paragraph("核心特性:",st["H3"])]
    for item in [
        "实现 ReadableConfig 和 WritableConfig 接口, 提供类型安全的配置读写",
        "实现 IOReadableWritable 接口, 支持Flink二进制序列化协议",
        "支持 ConfigOption 类型安全API和 String key 原始API两种访问方式",
        "线程安全: 所有操作通过 synchronized(confData) 保护",
        "支持 PrefixMap: 将嵌套配置展平为点分隔的扁平键",
        "支持 FallbackKey: 配置项可以有废弃键和回退键, 向后兼容",
        "支持 toFileWritableMap() 将配置写回YAML文件时自动补齐转义",
    ]:
        story+=[Paragraph("\u2022 "+item,st["BL"])]

    story+=[Paragraph("2.2 ConfigOption 类",st["H2"])]
    story+=[Paragraph("ConfigOption 描述单个配置项的完整元数据(279行), 是Flink配置框架的基石。每个 ConfigOption 是不可变的, 包含 key(当前键名)、clazz(值类型)、defaultValue(默认值)、fallbackKeys(回退键数组)和 description(描述)。",st["B"])]
    story+=[Paragraph("通过 ConfigOptions.key(\"xxx\").xxxType().defaultValue(yyy).withDescription(\"zzz\") 链式API构建, 典型使用场景如 CheckpointingOptions、TaskManagerOptions 等 60+ 个选项类。",st["B"])]

    story+=[Paragraph("2.3 GlobalConfiguration 类",st["H2"])]
    story+=[Paragraph("GlobalConfiguration(289行)是全局配置的加载入口。核心流程: 从 FLINK_CONF_DIR 环境变量定位配置目录 -> 读取 config.yaml -> YamlParserUtils 解析 -> flatten() 展平嵌套键 -> 合并动态参数。同时负责敏感信息过滤(password/secret/token等关键词匹配时输出 '******')。",st["B"])]
    story+=[PageBreak()]

    # ========== 三、类型系统 ==========
    story+=[Paragraph("三、类型系统与序列化体系",st["H1"])]
    story+=[Paragraph("Flink的类型系统是其高性能数据处理的基础。TypeInformation 描述类型元数据, TypeSerializer 负责二进制序列化, TypeSerializerSnapshot 支持状态兼容性演进。",st["B"])]

    story+=[fig_type_system()]
    story+=[Paragraph("图3: 类型系统三层架构 (TypeInformation -> TypeSerializer -> TypeSerializerSnapshot)",st["Cap"])]

    story+=[Paragraph("3.1 TypeInformation&lt;T&gt;",st["H2"])]
    story+=[Paragraph("TypeInformation(235行)是Flink类型系统的核心抽象类。它为所有用户函数的输入输出类型提供元数据, 桥接了Java对象模型与Flink的逻辑扁平化Schema。",st["B"])]
    story+=[Paragraph("核心职责:",st["H3"])]
    for item in [
        "isBasicType() / isTupleType(): 类型分类判断",
        "getArity(): 获取直接字段数; getTotalFields(): 获取递归扁平化后的总字段数",
        "getTypeClass(): 返回类型对应的Java Class",
        "isKeyType(): 判断是否可用作 join/grouping key",
        "createSerializer(config): 工厂方法, 创建对应的 TypeSerializer",
        "TypeInformation.of(Class) / of(TypeHint): 静态工厂, 通过 TypeExtractor 推断类型信息",
    ]:
        story+=[Paragraph("\u2022 "+item,st["BL"])]

    story+=[Paragraph("3.2 TypeSerializer&lt;T&gt;",st["H2"])]
    story+=[Paragraph("TypeSerializer(197行)是Flink序列化框架的核心抽象类。它定义了完整的序列化/反序列化/拷贝协议。每个实例非线程安全, 需通过 duplicate() 在多线程环境中使用。",st["B"])]
    story+=[mktable(
        [Paragraph(h,st["B"]) for h in ["方法","职责"]],
        [[Paragraph(c,st["B"]) for c in r] for r in [
            ["isImmutableType()","判断类型是否不可变"],
            ["duplicate()","创建深拷贝(有状态序列化器必须重写)"],
            ["createInstance()","创建类型空实例"],
            ["serialize(record, target)","将记录序列化到 DataOutputView"],
            ["deserialize(source)","从 DataInputView 反序列化记录"],
            ["copy(from) / copy(from, reuse)","深拷贝, 可复用实例"],
            ["copy(source, target)","二进制级拷贝(最高效)"],
            ["snapshotConfiguration()","快照序列化器配置用于Checkpoint"],
        ]],
        [150,300]
    )]

    story+=[Paragraph("3.3 TypeSerializerSnapshot&lt;T&gt;",st["H2"])]
    story+=[Paragraph("TypeSerializerSnapshot(170行)是Flink状态Schema演进的关键接口。它保存在Checkpoint中, 用于三个目的: 1)记录序列化器参数和Schema; 2)新序列化器兼容性检查; 3)在需要Schema迁移时作为读取序列化器的工厂。",st["B"])]
    story+=[Paragraph("核心方法: writeSnapshot()/readSnapshot() 序列化快照元数据; restoreSerializer() 从快照恢复可读序列化器; resolveSchemaCompatibility() 判断新旧序列化器兼容性(COMPATIBLE_AS_IS / COMPATIBLE_AFTER_MIGRATION / INCOMPATIBLE)。",st["B"])]
    story+=[PageBreak()]

    # ========== 四、文件系统 ==========
    story+=[Paragraph("四、文件系统抽象 (FileSystem)",st["H1"])]
    story+=[Paragraph("FileSystem(1117行)是Flink文件系统的核心抽象, 统一了本地文件系统、HDFS、S3、GCS、OSS等不同存储系统的访问接口。它被用于容错机制(状态和恢复数据存储)和内置连接器(文件Source/Sink)。",st["B"])]

    story+=[fig_filesystem()]
    story+=[Paragraph("图4: 文件系统抽象与SPI发现机制",st["Cap"])]

    story+=[Paragraph("4.1 核心设计",st["H2"])]
    for item in [
        "FSKey(scheme + authority)作为缓存键, HashMap CACHE 避免重复创建",
        "三级工厂发现: 1) FS_FACTORIES (ServiceLoader + PluginManager); 2) DIRECTLY_SUPPORTED_FILESYSTEM 映射; 3) FALLBACK_FACTORY (Hadoop)",
        "FileSystemSafetyNet: 安全网模式, 确保Task结束时自动关闭所有打开的流, 防止连接泄漏",
        "ConnectionLimitingFactory: 可配置连接数限制, 避免文件系统连接耗尽",
        "写入语义: 支持 NO_OVERWRITE 和 OVERWRITE 两种写模式, 不支持追加写入",
        "持久化契约: 可见性要求(close-to-open) + 持久性要求(由具体文件系统保证)",
    ]:
        story+=[Paragraph("\u2022 "+item,st["BL"])]

    story+=[Paragraph("4.2 Path 类",st["H2"])]
    story+=[Paragraph("Path(551行)是Flink的路径抽象, 基于URI实现。支持相对路径和绝对路径, 自动处理Windows驱动器字母、路径规范化(去除重复斜杠和尾部斜杠)。提供 parent/child 路径解析、makeQualified 路径限定、序列化/反序列化等功能。",st["B"])]
    story+=[PageBreak()]

    # ========== 五、内存管理 ==========
    story+=[Paragraph("五、内存管理 (MemorySegment)",st["H1"])]
    story+=[Paragraph("MemorySegment(1673行)是Flink内存管理的核心类, 功能上类似 ByteBuffer 但提供了更多专用能力。它统一封装了堆内(byte[])、堆外Direct(ByteBuffer)和堆外Unsafe三种内存模式, 通过 sun.misc.Unsafe 实现高效的原生内存操作。",st["B"])]

    story+=[fig_memory()]
    story+=[Paragraph("图5: MemorySegment 三种内存模式与核心操作",st["Cap"])]

    story+=[Paragraph("5.1 设计亮点",st["H2"])]
    for item in [
        "不使用继承分离不同内存类型: 避免虚方法调用开销, 单一 final 类实现所有逻辑",
        "折叠式边界检查: 用减法替代独立的范围校验, 同时检查边界和段释放状态",
        "原生字节序支持: 提供 BigEndian 和 LittleEndian 变体, 编译器可根据 LITTLE_ENDIAN 常量消除不适用的代码路径",
        "线程安全释放: 使用 AtomicBoolean isFreedAtomic 保证 free() 仅执行一次",
        "地址哨兵技巧: free() 后设置 address = addressLimit + 1, 使所有后续访问自动触发 IllegalStateException",
        "支持 MemorySegment 间直接拷贝(copyTo)和二进制比较(compare/equalTo), 避免数据拷贝到中间缓冲区",
    ]:
        story+=[Paragraph("\u2022 "+item,st["BL"])]

    story+=[Paragraph("5.2 DataInputView / DataOutputView",st["H2"])]
    story+=[Paragraph("DataInputView 和 DataOutputView 是 Flink 内部序列化的核心IO视图接口, 扩展了 Java 的 DataInput/DataOutput。MemorySegment 通过这些视图为 TypeSerializer 提供读写能力。DataInputDeserializer 和 DataOutputSerializer 是基于 byte[] 的轻量级实现, 在网络传输和状态序列化中广泛使用。",st["B"])]
    story+=[PageBreak()]

    # ========== 六、Source 连接器 ==========
    story+=[Paragraph("六、Source 连接器架构 (FLIP-27)",st["H1"])]
    story+=[Paragraph("Source 接口(100行)是 Flink 新一代数据源连接器的工厂接口(FLIP-27), 它将数据源的分片发现(SplitEnumerator)和数据读取(SourceReader)解耦, 分别运行在 JobManager 和 TaskManager 端。",st["B"])]

    story+=[fig_source_connector()]
    story+=[Paragraph("图6: Source 连接器双组件架构",st["Cap"])]

    story+=[Paragraph("6.1 Source 接口核心方法",st["H2"])]
    story+=[mktable(
        [Paragraph(h,st["B"]) for h in ["方法","职责"]],
        [[Paragraph(c,st["B"]) for c in r] for r in [
            ["getBoundedness()","返回 BOUNDED(批) 或 CONTINUOUS(流)"],
            ["createEnumerator()","创建新的分片枚举器(新作业启动)"],
            ["restoreEnumerator()","从Checkpoint恢复分片枚举器"],
            ["createReader()","继承自 SourceReaderFactory, 创建数据读取器"],
            ["getSplitSerializer()","创建分片的 SimpleVersionedSerializer"],
            ["getEnumeratorCheckpointSerializer()","创建枚举器状态的 SimpleVersionedSerializer"],
        ]],
        [180,280]
    )]

    story+=[Paragraph("6.2 SimpleVersionedSerializer",st["H2"])]
    story+=[Paragraph("SimpleVersionedSerializer(82行)是带版本号的简单序列化器接口, 用于 Source 连接器的分片和枚举器状态序列化。版本号机制使得序列化格式可以随时演进, 反序列化时根据版本号选择正确的解码逻辑。这比 TypeSerializer 更轻量, 适合元数据序列化场景。",st["B"])]
    story+=[PageBreak()]

    # ========== 七、作业执行 ==========
    story+=[Paragraph("七、作业执行流程",st["H1"])]
    story+=[Paragraph("flink-core 定义了作业执行的核心接口链: PipelineExecutor 负责提交执行, JobClient 提供作业控制, ExecutionConfig 管理执行参数。",st["B"])]

    story+=[fig_execution()]
    story+=[Paragraph("图7: 从用户代码到 JobClient 的完整执行链路",st["Cap"])]

    story+=[Paragraph("7.1 PipelineExecutor",st["H2"])]
    story+=[Paragraph("PipelineExecutor(55行)是执行器的核心接口, 其 execute() 方法接收 Pipeline(DAG) + Configuration + ClassLoader, 返回 CompletableFuture&lt;JobClient&gt;。通过 SPI 机制(PipelineExecutorFactory)发现具体实现, 如 LocalExecutor、RemoteExecutor、YarnSessionClusterExecutor 等。",st["B"])]

    story+=[Paragraph("7.2 JobClient",st["H2"])]
    story+=[Paragraph("JobClient(89行)是已提交作业的交互接口。提供: getJobID()/getJobStatus() 查询作业状态; cancel() 取消作业; triggerSavepoint()/stopWithSavepoint() 触发保存点; getAccumulators() 获取累加器; getJobExecutionResult() 等待最终结果。所有方法返回 CompletableFuture, 支持完全异步操作。",st["B"])]

    story+=[Paragraph("7.3 ExecutionConfig",st["H2"])]
    story+=[Paragraph("ExecutionConfig(717行)是作业执行的核心配置对象。管理: 并行度(parallelism)、最大并行度(maxParallelism)、闭包清理(ClosureCleaner)、Watermark 间隔、对象复用、重启策略、快照压缩等。内部委托给 Configuration 对象存储, 通过 configure(ReadableConfig) 方法从配置中批量加载所有选项。",st["B"])]
    story+=[PageBreak()]

    # ========== 八、状态描述符 ==========
    story+=[Paragraph("八、状态描述符 (StateDescriptor)",st["H1"])]
    story+=[Paragraph("StateDescriptor(450行)是 Flink 状态API的基础抽象类。每个有状态算子通过 StateDescriptor 描述需要的状态: 名称(name)、类型序列化器(TypeSerializer)、TTL配置(StateTtlConfig)。",st["B"])]

    story+=[Paragraph("8.1 状态类型枚举",st["H2"])]
    story+=[mktable(
        [Paragraph(h,st["B"]) for h in ["类型","描述","对应Descriptor"]],
        [[Paragraph(c,st["B"]) for c in r] for r in [
            ["VALUE","单值状态","ValueStateDescriptor"],
            ["LIST","列表状态","ListStateDescriptor"],
            ["MAP","映射状态","MapStateDescriptor"],
            ["REDUCING","归约状态","ReducingStateDescriptor"],
            ["AGGREGATING","聚合状态","AggregatingStateDescriptor"],
        ]],
        [80,150,160]
    )]

    story+=[Paragraph("8.2 核心设计",st["H2"])]
    for item in [
        "序列化器延迟初始化: 通过 AtomicReference + CAS 保证线程安全的单例初始化",
        "TTL 支持: enableTimeToLive(StateTtlConfig) 配置状态过期策略",
        "Queryable State: 通过 setQueryable() 使状态可被外部查询(已废弃)",
        "自定义序列化: 支持通过 ObjectInputStream/ObjectOutputStream 序列化默认值, 处理 transient 字段",
    ]:
        story+=[Paragraph("\u2022 "+item,st["BL"])]
    story+=[PageBreak()]

    # ========== 九、IO 与序列化 ==========
    story+=[Paragraph("九、IO 与序列化框架",st["H1"])]
    story+=[Paragraph("flink-core 的 IO 层(core.io 包)定义了数据输入切片和版本化序列化的基础抽象。",st["B"])]

    story+=[Paragraph("9.1 InputSplit 体系",st["H2"])]
    story+=[Paragraph("InputSplit 是 Flink 的数据切片抽象, 将数据源切分为可并行处理的片段。FileInputSplit 扩展了路径和字节范围信息。InputSplitAssigner 负责将切片分配给并行实例。GenericInputSplit 和 LocatableInputSplit 是两种基础实现, 后者携带了位置(host)信息用于数据本地性调度。",st["B"])]

    story+=[Paragraph("9.2 IOReadableWritable",st["H2"])]
    story+=[Paragraph("IOReadableWritable 是Flink内部二进制序列化的基础接口, 定义了 read(DataInputView)/write(DataOutputView) 方法。Configuration 和 Path 等核心类都实现此接口。该协议用于RPC通信、Checkpoint和TaskManager内部数据交换。",st["B"])]

    story+=[Paragraph("9.3 SerializerConfig",st["H2"])]
    story+=[Paragraph("SerializerConfig(87行)管理序列化器的全局配置: 注册的Kryo序列化器类、注册的POJO类型、TypeInfoFactory映射。控制是否禁用泛型类型(Kryo)、是否强制使用Kryo/Avro序列化POJO。通过 configure(ReadableConfig, ClassLoader) 从 PipelineOptions 加载配置。",st["B"])]
    story+=[PageBreak()]

    # ========== 十、插件系统 ==========
    story+=[Paragraph("十、插件系统 (Plugin)",st["H1"])]
    story+=[Paragraph("Flink的插件系统允许在独立的ClassLoader中加载扩展组件(如文件系统实现), 避免依赖冲突。",st["B"])]

    story+=[Paragraph("10.1 PluginManager",st["H2"])]
    story+=[Paragraph("PluginManager(39行)是插件管理器的核心接口, 只有一个方法: load(Class&lt;P&gt; service) 返回所有已知插件中给定SPI接口的实现迭代器。每个插件在独立的子ClassLoader中加载, 依赖与Flink运行时隔离。",st["B"])]

    story+=[Paragraph("10.2 插件发现与加载流程",st["H2"])]
    for item in [
        "插件目录: $FLINK_HOME/plugins/ 下每个子目录是一个独立插件",
        "每个插件目录包含独立的JAR包和依赖, 使用 PluginClassLoader 加载",
        "PluginDescriptorFinder 扫描插件目录, 为每个插件创建 PluginDescriptor",
        "PluginLoader 根据 PluginDescriptor 创建独立的 ClassLoader 并加载SPI",
        "FileSystem.initialize() 中通过 pluginManager.load(FileSystemFactory.class) 发现插件文件系统",
        "PluginFileSystemFactory 包装插件加载的工厂, 确保在正确的ClassLoader上下文中创建FileSystem",
    ]:
        story+=[Paragraph("\u2022 "+item,st["BL"])]
    story+=[PageBreak()]

    # ========== 十一、工具类 ==========
    story+=[Paragraph("十一、工具类体系",st["H1"])]
    story+=[Paragraph("util 包包含 85+ 个工具类, 是 Flink 最大的工具集合。以下列出最核心的工具类:",st["B"])]

    story+=[mktable(
        [Paragraph(h,st["B"]) for h in ["工具类","大小","核心功能"]],
        [[Paragraph(c,st["B"]) for c in r] for r in [
            ["Preconditions","13KB","前置条件检查: checkNotNull, checkArgument, checkState"],
            ["ExceptionUtils","28KB","异常处理: 链式异常、重新抛出、致命错误检测"],
            ["FileUtils","29KB","文件操作: 递归删除、原子移动、临时目录管理"],
            ["InstantiationUtil","27KB","反射实例化: 类加载、对象序列化/反序列化、深拷贝"],
            ["NetUtils","23KB","网络工具: 端口查找、主机名解析、URL编码"],
            ["StringUtils","15KB","字符串工具: 空值处理、十六进制转换、可读大小格式化"],
            ["CollectionUtil","10KB","集合工具: 预计算大小的HashMap创建、分区、null安全操作"],
            ["ParameterTool","12KB","参数解析: 命令行参数、Properties文件、系统属性"],
            ["FlinkSecurityManager","15KB","安全管理: System.exit保护、线程中断保护"],
        ]],
        [90,40,320]
    )]

    story+=[Paragraph("11.1 函数式工具 (util.function)",st["H2"])]
    story+=[Paragraph("提供可抛出异常的函数式接口: ThrowingConsumer、ThrowingRunnable、TriFunction、BiConsumerWithException 等, 弥补了 Java 标准函数式接口不能声明受检异常的不足, 在 Flink 内部广泛使用。",st["B"])]

    story+=[Paragraph("11.2 并发工具 (util.concurrent)",st["H2"])]
    story+=[Paragraph("FutureUtils 提供 CompletableFuture 的增强操作: 超时控制、重试机制、异常处理链。FlinkUserCodeClassLoaders 管理用户代码的类加载策略, 支持 parent-first 和 child-first 两种模式。",st["B"])]
    story+=[PageBreak()]

    # ========== 十二、设计模式 ==========
    story+=[Paragraph("十二、关键设计模式总结",st["H1"])]
    story+=[mktable(
        [Paragraph(h,st["B"]) for h in ["设计模式","应用场景","代码位置"]],
        [[Paragraph(c,st["B"]) for c in r] for r in [
            ["工厂方法","FileSystemFactory 创建FileSystem; TypeInformation.createSerializer()","FileSystem, TypeInformation"],
            ["SPI服务发现","ServiceLoader + PluginManager 发现FileSystem和Executor实现","FileSystem.initialize(), PipelineExecutorFactory"],
            ["Builder模式","ConfigOptions.key().xxxType().defaultValue().withDescription()","ConfigOption, ConfigOptions"],
            ["安全网/装饰器","FileSystemSafetyNet 包装FileSystem, 自动关闭泄漏的流","FileSystemSafetyNet"],
            ["延迟初始化","StateDescriptor 中 TypeSerializer 的 AtomicReference + CAS 延迟初始化","StateDescriptor"],
            ["快照/版本化","TypeSerializerSnapshot 记录序列化器状态, SimpleVersionedSerializer 带版本号","TypeSerializerSnapshot"],
            ["策略模式","ClosureCleanerLevel (NONE/TOP_LEVEL/RECURSIVE); WriteMode (NO_OVERWRITE/OVERWRITE)","ExecutionConfig, FileSystem"],
            ["模板方法","TypeSerializer 定义序列化协议模板, 子类实现具体序列化逻辑","TypeSerializer 及其 50+ 子类"],
            ["不可变对象","ConfigOption 一旦创建不可修改, withXxx() 返回新实例","ConfigOption"],
            ["缓存模式","FileSystem.CACHE 按 FSKey 缓存文件系统实例, 避免重复创建","FileSystem.CACHE"],
        ]],
        [80,200,170]
    )]

    story+=[Spacer(1,20)]
    story+=[Paragraph("RichFunction 生命周期",st["H2"])]
    story+=[Paragraph("RichFunction 定义了用户函数的完整生命周期: open(OpenContext) 初始化 -> 工作方法(map/filter等) -> close() 清理。通过 getRuntimeContext() 访问运行时上下文(并行度、累加器、分布式缓存)。这是所有有状态用户函数的基础接口。",st["B"])]

    story+=[Spacer(1,20)]
    story+=[HRFlowable(width="80%",thickness=1,color=colors.HexColor("#1565c0"),spaceAfter=12)]
    story+=[Paragraph("flink-core 作为 Flink 的基础核心模块, 以 500+ Java 文件定义了配置、类型、文件系统、内存、IO、插件、执行引擎和工具类等完整的底层基础设施。其精心设计的抽象层级(API -> 核心 -> 基础设施 -> 工具)使得上层模块可以专注于业务逻辑, 而由 flink-core 统一处理跨模块的共性需求。",st["B"])]

    # ========== 生成PDF ==========
    out=os.path.join(os.path.dirname(__file__),"Flink_Core_模块源码分析.pdf")
    doc=SimpleDocTemplate(out,pagesize=A4,leftMargin=2*cm,rightMargin=2*cm,topMargin=2*cm,bottomMargin=2*cm)
    doc.build(story)
    print(f"PDF generated: {out}")

if __name__=="__main__":
    build_pdf()
