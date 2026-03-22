#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Flink YARN 模块源码分析 - PDF 生成脚本"""

import os, math
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm, cm
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, HRFlowable)
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.graphics.shapes import Drawing, Line, Rect, String, Polygon

# === 字体注册 ===
FONT_CN = 'Helvetica'
FONT_CN_B = 'Helvetica-Bold'
for fp in ['/System/Library/Fonts/PingFang.ttc', '/System/Library/Fonts/STHeiti Light.ttc',
           '/System/Library/Fonts/Hiragino Sans GB.ttc', '/Library/Fonts/Arial Unicode.ttf']:
    if os.path.exists(fp):
        try:
            pdfmetrics.registerFont(TTFont('CN', fp, subfontIndex=0) if fp.endswith('.ttc') else TTFont('CN', fp))
            FONT_CN = FONT_CN_B = 'CN'
            break
        except: continue

def S(): return getSampleStyleSheet()

def mkstyles():
    s = S()
    def a(n,**kw): s.add(ParagraphStyle(name=n,**kw))
    a('T', fontName=FONT_CN_B, fontSize=26, leading=36, alignment=TA_CENTER, spaceAfter=20, textColor=colors.HexColor('#1a237e'))
    a('ST', fontName=FONT_CN, fontSize=14, leading=20, alignment=TA_CENTER, spaceAfter=10, textColor=colors.HexColor('#455a64'))
    a('H1', fontName=FONT_CN_B, fontSize=20, leading=28, spaceBefore=24, spaceAfter=12, textColor=colors.HexColor('#1565c0'))
    a('H2', fontName=FONT_CN_B, fontSize=16, leading=22, spaceBefore=16, spaceAfter=8, textColor=colors.HexColor('#1976d2'))
    a('H3', fontName=FONT_CN_B, fontSize=13, leading=18, spaceBefore=12, spaceAfter=6, textColor=colors.HexColor('#1e88e5'))
    a('B', fontName=FONT_CN, fontSize=10.5, leading=17, spaceBefore=4, spaceAfter=4, alignment=TA_JUSTIFY, textColor=colors.HexColor('#212121'))
    a('BI', fontName=FONT_CN, fontSize=10.5, leading=17, spaceBefore=2, spaceAfter=2, leftIndent=20, textColor=colors.HexColor('#212121'))
    a('BL', fontName=FONT_CN, fontSize=10.5, leading=17, spaceBefore=2, spaceAfter=2, leftIndent=30, bulletIndent=15, textColor=colors.HexColor('#212121'))
    a('Cap', fontName=FONT_CN, fontSize=9, leading=13, alignment=TA_CENTER, spaceAfter=12, textColor=colors.HexColor('#757575'))
    return s

# === 绘图工具 ===
def box(d,x,y,w,h,txt,fc,tc=colors.white,fs=9):
    d.add(Rect(x,y,w,h,rx=5,ry=5,fillColor=fc,strokeColor=colors.HexColor('#90a4ae'),strokeWidth=0.5))
    lines=txt.split('\n')
    sy=y+h/2+(len(lines)*(fs+2))/2-fs/2
    for i,l in enumerate(lines):
        d.add(String(x+w/2,sy-i*(fs+2),l,fontName=FONT_CN,fontSize=fs,fillColor=tc,textAnchor='middle'))

def arrow(d,x1,y1,x2,y2,sc=colors.HexColor('#546e7a'),sw=1.2):
    d.add(Line(x1,y1,x2,y2,strokeColor=sc,strokeWidth=sw))
    a=math.atan2(y2-y1,x2-x1); al=6; aa=math.pi/7
    d.add(Polygon([x2,y2,x2-al*math.cos(a-aa),y2-al*math.sin(a-aa),x2-al*math.cos(a+aa),y2-al*math.sin(a+aa)],fillColor=sc,strokeColor=sc,strokeWidth=0.5))

def label(d,x,y,t,fs=8,c=colors.HexColor('#455a64')):
    d.add(String(x,y,t,fontName=FONT_CN,fontSize=fs,fillColor=c,textAnchor='middle'))

# === 图表 ===
def fig_deploy():
    d=Drawing(500,560)
    d.add(Rect(0,0,500,560,fillColor=colors.HexColor('#fafafa'),strokeColor=colors.HexColor('#e0e0e0'),strokeWidth=0.5))
    label(d,250,540,'Flink on YARN 集群部署核心流程',13,colors.HexColor('#1565c0'))
    bw,bh,cx=160,30,170
    steps=[('1. CLI 解析命令行参数','#1565c0'),('2. 创建 YarnClusterDescriptor','#1976d2'),
           ('3. isReadyForDeployment 检查','#1e88e5'),('4. 验证YARN资源和队列','#2196f3'),
           ('5. 上传文件到HDFS','#42a5f5'),('6. 构建AM启动命令','#1e88e5'),
           ('7. 提交Application到YARN','#1976d2'),('8. 轮询等待AM启动','#1565c0'),
           ('9. 返回 RestClusterClient','#0d47a1')]
    notes=['FlinkYarnSessionCli','YarnClusterClientFactory','检查flinkJarPath/vCores',
           'checkYarnQueues()','YarnAppFileUploader','setupAMContainer()',
           'yarnClient.submit()','NEW->RUNNING','RestClusterClient']
    for i,(s,c) in enumerate(steps):
        y=490-i*50
        box(d,cx,y,bw,bh,s,colors.HexColor(c),fs=8)
        if i>0: arrow(d,cx+bw/2,y+50,cx+bw/2,y+bh)
        box(d,360,y,110,bh,notes[i],colors.HexColor('#e3f2fd'),colors.HexColor('#1565c0'),7)
        arrow(d,cx+bw,y+bh/2,360,y+bh/2,colors.HexColor('#90caf9'),0.8)
    return d

def fig_resource():
    d=Drawing(500,440)
    d.add(Rect(0,0,500,440,fillColor=colors.HexColor('#fafafa'),strokeColor=colors.HexColor('#e0e0e0'),strokeWidth=0.5))
    label(d,250,420,'YARN 资源管理核心流程',13,colors.HexColor('#1565c0'))
    bw,bh=130,28
    # Flink RM side
    label(d,80,395,'Flink ResourceManager',10,colors.HexColor('#e65100'))
    items_l=[('initializeInternal()','#e65100',370),('registerAppMaster','#f57c00',330),
             ('requestResource()','#fb8c00',290),('addContainerRequest()','#ff9800',250)]
    for t,c,y in items_l:
        box(d,15,y,bw,bh,t,colors.HexColor(c),fs=8)
    for i in range(1,len(items_l)):
        arrow(d,15+bw/2,items_l[i-1][2],15+bw/2,items_l[i][2]+bh)
    # YARN RM side
    label(d,255,395,'YARN RM (AMRMClientAsync)',10,colors.HexColor('#2e7d32'))
    box(d,185,330,bw,bh,'AMRMClientAsync',colors.HexColor('#2e7d32'),fs=8)
    box(d,185,250,bw,bh,'onContainersAllocated',colors.HexColor('#43a047'),fs=8)
    arrow(d,185+bw/2,330,185+bw/2,250+bh,colors.HexColor('#66bb6a'))
    arrow(d,15+bw,250+bh/2,185,330+bh/2,colors.HexColor('#ff9800'))
    # NM side
    label(d,420,395,'NodeManager (NMClient)',10,colors.HexColor('#6a1b9a'))
    box(d,355,290,bw,bh,'NMClientAsync',colors.HexColor('#6a1b9a'),fs=8)
    box(d,355,250,bw,bh,'startContainerAsync',colors.HexColor('#7b1fa2'),fs=8)
    arrow(d,355+bw/2,290,355+bw/2,250+bh,colors.HexColor('#ab47bc'))
    arrow(d,185+bw,250+bh/2,355,290+bh/2,colors.HexColor('#43a047'))
    # TaskExecutor
    box(d,185,190,bw,bh,'createTaskExecutor\nLaunchContext',colors.HexColor('#1565c0'),fs=7)
    arrow(d,185+bw/2,250,185+bw/2,190+bh,colors.HexColor('#42a5f5'))
    box(d,185,140,bw,bh,'TaskExecutor 启动',colors.HexColor('#0d47a1'),fs=8)
    arrow(d,185+bw/2,190,185+bw/2,140+bh,colors.HexColor('#42a5f5'))
    # Complete
    box(d,15,140,bw,bh,'onContainersCompleted',colors.HexColor('#c62828'),fs=8)
    arrow(d,185,155,15+bw,155,colors.HexColor('#ef5350'))
    box(d,15,90,bw,bh,'releaseResource()',colors.HexColor('#b71c1c'),fs=8)
    arrow(d,15+bw/2,140,15+bw/2,90+bh,colors.HexColor('#ef5350'))
    # Heartbeat
    label(d,250,70,'心跳自适应: 有请求时快心跳, 无请求时慢心跳',8,colors.HexColor('#9e9e9e'))
    return d

def fig_modes():
    d=Drawing(500,340)
    d.add(Rect(0,0,500,340,fillColor=colors.HexColor('#fafafa'),strokeColor=colors.HexColor('#e0e0e0'),strokeWidth=0.5))
    label(d,250,320,'Session 模式 vs Application 模式',13,colors.HexColor('#1565c0'))
    # Session
    label(d,120,295,'Session 模式',10,colors.HexColor('#e65100'))
    box(d,20,260,90,26,'Flink Client',colors.HexColor('#e65100'),fs=8)
    box(d,140,260,100,26,'Session Cluster',colors.HexColor('#f57c00'),fs=8)
    arrow(d,110,273,140,273)
    box(d,140,225,100,26,'JM (AM) 常驻',colors.HexColor('#fb8c00'),fs=8)
    box(d,140,190,100,26,'TMs 共享',colors.HexColor('#ff9800'),fs=8)
    arrow(d,190,260,190,251,colors.HexColor('#ffb74d'))
    arrow(d,190,225,190,216,colors.HexColor('#ffb74d'))
    label(d,120,175,'多Job共享资源',7,colors.HexColor('#9e9e9e'))
    # Application
    label(d,380,295,'Application 模式',10,colors.HexColor('#1565c0'))
    box(d,290,260,90,26,'Flink Client',colors.HexColor('#1565c0'),fs=8)
    box(d,400,260,90,26,'App Cluster 1',colors.HexColor('#1976d2'),fs=8)
    arrow(d,380,273,400,273)
    box(d,400,225,90,26,'AM+main()',colors.HexColor('#2196f3'),fs=8)
    box(d,400,190,90,26,'独立 TMs',colors.HexColor('#42a5f5'),fs=8)
    arrow(d,445,260,445,251,colors.HexColor('#64b5f6'))
    arrow(d,445,225,445,216,colors.HexColor('#64b5f6'))
    label(d,380,175,'Job级别资源隔离',7,colors.HexColor('#9e9e9e'))
    # Table
    label(d,250,145,'关键差异对比',10,colors.HexColor('#455a64'))
    d.add(Rect(20,10,460,125,fillColor=colors.HexColor('#eceff1'),strokeColor=colors.HexColor('#b0bec5'),strokeWidth=0.5,rx=3,ry=3))
    items=[('入口点','YarnSessionClusterEntrypoint','YarnApplicationClusterEntryPoint'),
           ('Job图','客户端生成并提交','AM内执行main()生成'),
           ('资源隔离','共享集群, 无隔离','独立集群, 完全隔离'),
           ('Entrypoint类','SessionClusterEntrypoint','ApplicationClusterEntryPoint'),
           ('适用场景','交互式开发/多短Job','生产环境/大型Job')]
    for i,(l,s,a) in enumerate(items):
        yy=115-i*20
        label(d,55,yy,l,7,colors.HexColor('#455a64'))
        label(d,180,yy,s,7,colors.HexColor('#e65100'))
        label(d,380,yy,a,7,colors.HexColor('#1565c0'))
    return d

def fig_upload():
    d=Drawing(500,300)
    d.add(Rect(0,0,500,300,fillColor=colors.HexColor('#fafafa'),strokeColor=colors.HexColor('#e0e0e0'),strokeWidth=0.5))
    label(d,250,280,'文件上传与资源注册流程',13,colors.HexColor('#1565c0'))
    label(d,70,255,'本地文件',10,colors.HexColor('#2e7d32'))
    for i,f in enumerate(['flink-dist.jar','lib/*.jar','plugins/','config.yaml','user jars']):
        box(d,10,225-i*30,120,24,f,colors.HexColor('#c8e6c9'),colors.HexColor('#2e7d32'),7)
    box(d,170,175,140,35,'YarnApplication\nFileUploader',colors.HexColor('#1565c0'),fs=9)
    arrow(d,130,195,170,195,colors.HexColor('#66bb6a'))
    label(d,420,255,'HDFS',10,colors.HexColor('#e65100'))
    box(d,360,215,130,28,'.flink/<appId>/',colors.HexColor('#fff3e0'),colors.HexColor('#e65100'),8)
    arrow(d,310,195,360,229,colors.HexColor('#ff9800'))
    label(d,250,120,'注册 YARN LocalResource',10,colors.HexColor('#6a1b9a'))
    for i,(n,desc) in enumerate([('APPLICATION可见性','本应用独享'),('PUBLIC可见性','跨应用共享缓存'),('Classpath构建','系统+用户路径')]):
        y=90-i*30
        box(d,80,y,140,24,n,colors.HexColor('#6a1b9a'),fs=8)
        box(d,250,y,160,24,desc,colors.HexColor('#ce93d8'),colors.HexColor('#4a148c'),7)
        arrow(d,220,y+12,250,y+12,colors.HexColor('#ab47bc'))
    arrow(d,240,175,240,120,colors.HexColor('#7b1fa2'))
    return d

def fig_class():
    d=Drawing(500,420)
    d.add(Rect(0,0,500,420,fillColor=colors.HexColor('#fafafa'),strokeColor=colors.HexColor('#e0e0e0'),strokeWidth=0.5))
    label(d,250,400,'核心类层次结构',13,colors.HexColor('#1565c0'))
    bw=175
    layers=[
        ('集群部署层','#e65100',[('ClusterDescriptor<T>','#fff3e0','#e65100'),('YarnClusterDescriptor','#e65100','#ffffff')]),
        ('入口点层','#2e7d32',[('ClusterEntrypoint','#e8f5e9','#2e7d32'),('YarnSession/AppEntryPoint','#2e7d32','#ffffff')]),
        ('资源管理层','#1565c0',[('AbstractRMDriver<T>','#e3f2fd','#1565c0'),('YarnResourceManagerDriver','#1565c0','#ffffff')]),
        ('CLI层','#6a1b9a',[('AbstractYarnCli','#f3e5f5','#6a1b9a'),('FlinkYarnSessionCli','#6a1b9a','#ffffff')]),
        ('工具适配层','#00695c',[('Utils / YarnConfigOptions','#00695c','#ffffff'),('TaskExecSpecAdapter','#00897b','#ffffff')]),
    ]
    for i,(lbl,lc,classes) in enumerate(layers):
        y=360-i*70
        label(d,60,y+10,lbl,10,colors.HexColor(lc))
        for j,(cn,bg,tc) in enumerate(classes):
            box(d,10+j*200,y-25,bw,24,cn,colors.HexColor(bg),colors.HexColor(tc),8)
        if len(classes)==2:
            arrow(d,10+bw,y-13,210,y-13)
    return d

# === 构建PDF ===
def build():
    out=os.path.join(os.path.dirname(os.path.abspath(__file__)),'Flink_YARN_模块源码分析.pdf')
    doc=SimpleDocTemplate(out,pagesize=A4,rightMargin=2*cm,leftMargin=2*cm,topMargin=2.5*cm,bottomMargin=2*cm)
    st=mkstyles()
    story=[]

    # 封面
    story+=[Spacer(1,80),Paragraph('Flink YARN 模块',st['T']),Paragraph('核心流程与设计说明',st['T']),
            Spacer(1,20),HRFlowable(width="60%",thickness=2,color=colors.HexColor('#1565c0')),Spacer(1,20),
            Paragraph('基于 Apache Flink 1.15.4 Release 分支源码分析',st['ST']),
            Paragraph('flink-yarn 模块完整解读',st['ST']),Spacer(1,40),
            Paragraph('模块路径: flink-yarn/src/main/java/org/apache/flink/yarn/',st['ST']),PageBreak()]

    # 目录
    story+=[Paragraph('目录',st['H1']),Spacer(1,10)]
    for t in ['一、模块概览与架构设计','二、核心类设计说明','三、集群部署核心流程','四、资源管理核心流程',
              '五、Session vs Application 模式','六、文件上传与资源注册','七、CLI 命令行交互',
              '八、配置体系','九、安全机制','十、关键设计模式']:
        story+=[Paragraph(t,st['B']),Spacer(1,3)]
    story+=[PageBreak()]

    # 第一章
    story+=[Paragraph('一、模块概览与架构设计',st['H1']),HRFlowable(width="100%",thickness=1,color=colors.HexColor('#e0e0e0'))]
    story+=[Paragraph('1.1 模块定位',st['H2'])]
    story+=[Paragraph('flink-yarn 模块是 Apache Flink 与 Hadoop YARN 集成的核心桥梁。负责将 Flink 集群部署到 YARN，管理 YARN 容器中 TaskExecutor 进程的生命周期，处理所有 YARN 特有的资源分配、文件分发和安全认证。',st['B'])]
    story+=[Paragraph('1.2 模块包结构',st['H2'])]
    t=Table([['包名','职责','核心类数'],
             ['o.a.f.yarn','核心实现: 集群描述、资源管理、工具','15+'],
             ['o.a.f.yarn.cli','CLI 命令行交互','4'],
             ['o.a.f.yarn.configuration','YARN 配置项','5'],
             ['o.a.f.yarn.entrypoint','AM 入口点','5'],
             ['o.a.f.yarn.executors','执行器工厂','2']],colWidths=[130,220,80])
    t.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),colors.HexColor('#1565c0')),('TEXTCOLOR',(0,0),(-1,0),colors.white),
        ('FONTNAME',(0,0),(-1,-1),FONT_CN),('FONTSIZE',(0,0),(-1,-1),9),('ALIGN',(0,0),(-1,-1),'CENTER'),
        ('GRID',(0,0),(-1,-1),0.5,colors.HexColor('#b0bec5')),('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.HexColor('#fafafa'),colors.HexColor('#f5f5f5')]),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),('TOPPADDING',(0,0),(-1,-1),5),('BOTTOMPADDING',(0,0),(-1,-1),5)]))
    story+=[t,Spacer(1,10)]
    story+=[Paragraph('1.3 核心类层次结构',st['H2'])]
    story+=[fig_class(),Paragraph('图 1-1: 核心类层次结构',st['Cap']),PageBreak()]

    # 第二章
    story+=[Paragraph('二、核心类设计说明',st['H1']),HRFlowable(width="100%",thickness=1,color=colors.HexColor('#e0e0e0'))]

    story+=[Paragraph('2.1 YarnClusterDescriptor (约1900行)',st['H2'])]
    story+=[Paragraph('实现 ClusterDescriptor&lt;ApplicationId&gt; 接口，是集群部署的核心协调者。',st['B'])]
    for r in ['deploySessionCluster()/deployApplicationCluster(): 两种部署模式统一入口，内部调用 deployInternal()',
              'isReadyForDeployment(): 检查 flinkJarPath、YARN vCores 上限、Hadoop 环境变量',
              'validateClusterResources(): 内存对齐(yarn.scheduler.minimum-allocation-mb)、最大资源限制',
              'startAppMaster(): 核心方法，上传文件、构建AM容器、提交到YARN并轮询等待RUNNING',
              'setupApplicationMasterContainer(): 通过模板替换构建JVM启动命令',
              'DeploymentFailureHook: 内部Thread子类，注册ShutdownHook防止部署失败时资源泄漏',
              'ApplicationSubmissionContextReflector: 反射兼容不同Hadoop版本(2.4.0+/2.6.0+)',
              'setTokensFor(): Kerberos delegation token 注入到容器上下文']:
        story+=[Paragraph(f'  \u2022 {r}',st['BL'])]

    story+=[Paragraph('2.2 YarnResourceManagerDriver (约880行)',st['H2'])]
    story+=[Paragraph('继承 AbstractResourceManagerDriver&lt;YarnWorkerNode&gt;，运行在AM进程中，负责动态容器管理。',st['B'])]
    for r in ['双客户端架构: AMRMClientAsync(与YARN RM通信) + NMClientAsync(与NodeManager通信)',
              '心跳自适应: 有容器请求时快心跳，无请求时慢心跳，减少YARN RM负担',
              'requestResourceFutures: Map<TaskExecutorProcessSpec, Queue<CompletableFuture>> 异步资源请求队列',
              'AMRMCallbackHandler: 处理 onContainersAllocated/onContainersCompleted/onShutdownRequest',
              'NMCallbackHandler: 处理容器启动/停止回调，使用Phaser跟踪释放进度',
              'getContainersFromPreviousAttempts(): AM 容器恢复(HA场景)',
              'tryUpdateApplicationBlockList(): 动态节点黑名单管理']:
        story+=[Paragraph(f'  \u2022 {r}',st['BL'])]

    story+=[Paragraph('2.3 YarnApplicationFileUploader (约560行)',st['H2'])]
    story+=[Paragraph('负责文件上传到HDFS(.flink/&lt;appId&gt;/)并注册为YARN LocalResource，实现AutoCloseable。',st['B'])]
    for r in ['registerSingleLocalResource(): 上传单文件，支持APPLICATION/PUBLIC可见性',
              'registerMultipleLocalResources(): 递归处理目录，排除flink-dist*.jar，构建classpath',
              'uploadFlinkDist(): 单独处理Flink发行版JAR，确保不重复',
              'registerProvidedLocalResources(): PUBLIC可见性的共享库，支持跨应用YARN缓存',
              'waitForTransferToComplete(): 处理HDFS最终一致性(重试3次)']:
        story+=[Paragraph(f'  \u2022 {r}',st['BL'])]

    story+=[Paragraph('2.4 FlinkYarnSessionCli (约977行)',st['H2'])]
    story+=[Paragraph('YARN Session 模式命令行入口，继承 AbstractYarnCli。',st['B'])]
    for r in ['解析参数: -jm/-tm(内存), -s(slots), -qu(队列), -nm(名称), -D(动态属性)',
              'YARN Properties 文件: .yarn-properties-{user} 支持跨进程会话恢复',
              'runInteractiveCli(): 交互式CLI，help/stop命令，持续监控应用状态',
              'run(): 主流程 — 创建描述符 -> 部署/检索集群 -> 写属性文件 -> 交互/detach']:
        story+=[Paragraph(f'  \u2022 {r}',st['BL'])]

    story+=[Paragraph('2.5 Utils 工具类 (约823行)',st['H2'])]
    for r in ['createTaskExecutorContext(): 构建TM容器完整上下文(LocalResource+环境变量+安全令牌)',
              'getTaskManagerShellCommand(): 模板替换生成JVM启动命令',
              'setupYarnClassPath(): 配置YARN类路径',
              'getYarnAndHadoopConfiguration(): 合并Flink和Hadoop配置',
              'setAclsFor(): 设置YARN应用ACL(VIEW_APP/MODIFY_APP)']:
        story+=[Paragraph(f'  \u2022 {r}',st['BL'])]

    story+=[Paragraph('2.6 其他核心类',st['H2'])]
    t2=Table([['类名','职责'],
              ['YarnWorkerNode','封装YARN Container和ResourceID，代表一个TaskExecutor'],
              ['TaskExecutorProcessSpec\nContainerResourcePriority\nAdapter','Flink TaskExecutorProcessSpec 与 YARN Resource/Priority 的双向映射适配器'],
              ['YarnConfigOptions','526行配置项定义: 队列、内存、端口、安全、文件分发等全部YARN配置'],
              ['YarnDeploymentTarget','枚举: SESSION("yarn-session") / APPLICATION("yarn-application")'],
              ['YarnResourceManagerFactory','单例工厂，创建 YarnResourceManagerDriver，不支持多Leader会话'],
              ['YarnEntrypointUtils','AM入口工具: 加载配置、解析keytab路径、日志YARN环境信息'],
              ['YarnLocalResourceDescriptor','YARN本地资源描述符: 封装路径/大小/时间戳/可见性/类型，支持序列化']],colWidths=[150,290])
    t2.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),colors.HexColor('#1565c0')),('TEXTCOLOR',(0,0),(-1,0),colors.white),
        ('FONTNAME',(0,0),(-1,-1),FONT_CN),('FONTSIZE',(0,0),(-1,-1),8),('ALIGN',(0,0),(0,-1),'LEFT'),('ALIGN',(1,0),(1,-1),'LEFT'),
        ('GRID',(0,0),(-1,-1),0.5,colors.HexColor('#b0bec5')),('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.HexColor('#fafafa'),colors.HexColor('#f5f5f5')]),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),('TOPPADDING',(0,0),(-1,-1),4),('BOTTOMPADDING',(0,0),(-1,-1),4)]))
    story+=[t2,PageBreak()]

    # 第三章
    story+=[Paragraph('三、集群部署核心流程',st['H1']),HRFlowable(width="100%",thickness=1,color=colors.HexColor('#e0e0e0'))]
    story+=[Paragraph('集群部署是 flink-yarn 模块最核心的流程，由 YarnClusterDescriptor.deployInternal() 方法驱动。完整流程包括 Kerberos 安全检查、YARN 资源验证、文件上传、AM 容器构建和应用提交。',st['B'])]
    story+=[fig_deploy(),Paragraph('图 3-1: 集群部署核心流程',st['Cap'])]
    story+=[Paragraph('关键实现细节:',st['H3'])]
    for d in ['deployInternal() 先检查 Kerberos 凭据有效性，若启用安全但凭据无效则抛出异常',
              'YARN 资源验证包括: 内存对齐到 yarn.scheduler.minimum-allocation-mb 的整数倍、不超过集群最大容器资源',
              'startAppMaster() 是最长的方法(约480行): 初始化文件系统 -> 创建FileUploader -> 上传系统/用户文件 -> 构建classpath -> 写JobGraph/配置 -> 处理Kerberos -> 创建ContainerLaunchContext -> 提交 -> 轮询状态',
              '提交后进入轮询循环(250ms间隔): NEW -> SUBMITTED -> ACCEPTED -> RUNNING (成功) 或 FAILED/KILLED (失败)',
              'DeploymentFailureHook 在轮询过程中保护资源，部署成功后移除']:
        story+=[Paragraph(f'  \u2022 {d}',st['BL'])]
    story+=[PageBreak()]

    # 第四章
    story+=[Paragraph('四、资源管理核心流程',st['H1']),HRFlowable(width="100%",thickness=1,color=colors.HexColor('#e0e0e0'))]
    story+=[Paragraph('资源管理流程运行在 AM 进程内部，由 YarnResourceManagerDriver 驱动，通过 YARN 的异步客户端与 YARN RM 和 NodeManager 交互。',st['B'])]
    story+=[fig_resource(),Paragraph('图 4-1: 资源管理核心流程',st['Cap'])]
    story+=[Paragraph('核心交互流程:',st['H3'])]
    for d in ['初始化: 创建并启动 AMRMClientAsync 和 NMClientAsync，向YARN RM注册AM',
              '资源请求: Flink SlotManager 通过 requestResource() 发起，转化为 YARN ContainerRequest',
              'TaskExecutorProcessSpecContainerResourcePriorityAdapter 将 Flink 的 TaskExecutorProcessSpec 转为 YARN Resource+Priority',
              '容器分配回调: onContainersAllocated() 按Priority分组处理，匹配到 requestResourceFutures 队列',
              '容器启动: createTaskExecutorLaunchContext() 在IO线程池异步构建，通过 NMClientAsync 启动',
              '容器完成: onContainersCompleted() 通知 ResourceEventHandler，处理不同退出状态(PREEMPTED/ABORTED/DISKS_FAILED等)',
              '容器释放: releaseResource() 同时通知 NM 停止容器和通知 RM 释放分配',
              '优雅终止: terminate() 使用 Phaser 等待所有容器停止后再关闭客户端']:
        story+=[Paragraph(f'  \u2022 {d}',st['BL'])]
    story+=[PageBreak()]

    # 第五章
    story+=[Paragraph('五、Session 模式 vs Application 模式',st['H1']),HRFlowable(width="100%",thickness=1,color=colors.HexColor('#e0e0e0'))]
    story+=[Paragraph('Flink on YARN 支持两种部署模式，通过 YarnDeploymentTarget 枚举区分。两种模式在入口点、Job图生成位置和资源隔离性上有本质差异。',st['B'])]
    story+=[fig_modes(),Paragraph('图 5-1: Session 模式 vs Application 模式对比',st['Cap'])]
    story+=[Paragraph('Session 模式详解:',st['H3'])]
    for d in ['入口点: YarnSessionClusterEntrypoint -> SessionClusterEntrypoint',
              '部署: deploySessionCluster() -> deployInternal()，AM启动后等待Job提交',
              'Job图由客户端生成并通过REST API提交到已运行的Session集群',
              '多个Job共享同一个JM和TM资源池，适合交互式开发和短任务']:
        story+=[Paragraph(f'  \u2022 {d}',st['BL'])]
    story+=[Paragraph('Application 模式详解:',st['H3'])]
    for d in ['入口点: YarnApplicationClusterEntryPoint -> ApplicationClusterEntryPoint',
              '部署: deployApplicationCluster()，用户JAR被上传到AM，在AM内执行main()生成Job图',
              '每个Application独立拥有JM和TM，Job级别完全隔离',
              'AM内通过 DefaultPackagedProgramRetriever 获取 PackagedProgram 并执行']:
        story+=[Paragraph(f'  \u2022 {d}',st['BL'])]
    story+=[PageBreak()]

    # 第六章
    story+=[Paragraph('六、文件上传与资源注册',st['H1']),HRFlowable(width="100%",thickness=1,color=colors.HexColor('#e0e0e0'))]
    story+=[Paragraph('文件上传由 YarnApplicationFileUploader 管理，是部署流程中最复杂的环节之一。需要将本地的JAR包、配置文件、插件等上传到HDFS，并注册为YARN LocalResource。',st['B'])]
    story+=[fig_upload(),Paragraph('图 6-1: 文件上传与资源注册流程',st['Cap'])]
    story+=[Paragraph('Classpath 构建策略:',st['H3'])]
    for d in ['UserJarInclusion.FIRST: 用户JAR排在系统JAR之前',
              'UserJarInclusion.ORDER: 用户JAR混入系统JAR一起排序',
              'UserJarInclusion.LAST: 用户JAR排在系统JAR之后(默认)',
              'UserJarInclusion.DISABLED: 用户JAR放入usrlib目录，由UserClassLoader加载']:
        story+=[Paragraph(f'  \u2022 {d}',st['BL'])]
    story+=[PageBreak()]

    # 第七章
    story+=[Paragraph('七、CLI 命令行交互',st['H1']),HRFlowable(width="100%",thickness=1,color=colors.HexColor('#e0e0e0'))]
    story+=[Paragraph('FlinkYarnSessionCli 提供了完整的 YARN Session 管理命令行界面。',st['B'])]
    t3=Table([['参数','说明'],
              ['-jm / --jobManagerMemory','JobManager 内存，支持单位(1024m, 1g)'],
              ['-tm / --taskManagerMemory','TaskManager 内存'],
              ['-s / --slots','每个 TaskManager 的 slot 数'],
              ['-qu / --queue','YARN 队列名'],
              ['-nm / --name','应用名称'],
              ['-at / --applicationType','应用类型'],
              ['-nl / --nodeLabel','YARN 节点标签'],
              ['-z / --zookeeperNamespace','ZK 命名空间(HA模式)'],
              ['-id / --applicationId','连接已有Session'],
              ['-d / --detached','分离模式(后台运行)'],
              ['-D property=value','动态配置属性']],colWidths=[160,280])
    t3.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),colors.HexColor('#1565c0')),('TEXTCOLOR',(0,0),(-1,0),colors.white),
        ('FONTNAME',(0,0),(-1,-1),FONT_CN),('FONTSIZE',(0,0),(-1,-1),9),('ALIGN',(0,0),(-1,-1),'LEFT'),
        ('GRID',(0,0),(-1,-1),0.5,colors.HexColor('#b0bec5')),('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.HexColor('#fafafa'),colors.HexColor('#f5f5f5')]),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),('TOPPADDING',(0,0),(-1,-1),4),('BOTTOMPADDING',(0,0),(-1,-1),4)]))
    story+=[t3,PageBreak()]

    # 第八章
    story+=[Paragraph('八、配置体系',st['H1']),HRFlowable(width="100%",thickness=1,color=colors.HexColor('#e0e0e0'))]
    story+=[Paragraph('YarnConfigOptions 定义了526行YARN专有配置，是模块配置的中枢。',st['B'])]
    t4=Table([['配置分类','关键配置项','说明'],
              ['资源配置','yarn.appmaster.vcores\nyarn.containers.vcores','AM/TM 虚拟核心数'],
              ['队列配置','yarn.application.queue','YARN 队列'],
              ['文件分发','yarn.ship-files\nyarn.ship-archives\nyarn.provided-lib-dirs','本地文件/归档/共享库'],
              ['安全配置','yarn.ship-local-keytab\nyarn.security.kerberos.localized-keytab-path','Keytab 分发策略'],
              ['AM端口','yarn.application-master.port','AM RPC 端口范围'],
              ['心跳','yarn.heartbeat-delay\nyarn.container-request-heartbeat-interval-milliseconds','心跳间隔配置'],
              ['高可用','yarn.application-attempts\nyarn.application-attempt-failures-validity-interval','HA 重试配置'],
              ['日志','yarn.rolled-log-include-pattern\nyarn.rolled-log-exclude-pattern','日志聚合过滤'],
              ['部署模式','execution.target','yarn-session / yarn-application']],colWidths=[80,170,190])
    t4.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),colors.HexColor('#1565c0')),('TEXTCOLOR',(0,0),(-1,0),colors.white),
        ('FONTNAME',(0,0),(-1,-1),FONT_CN),('FONTSIZE',(0,0),(-1,-1),8),('ALIGN',(0,0),(-1,-1),'LEFT'),
        ('GRID',(0,0),(-1,-1),0.5,colors.HexColor('#b0bec5')),('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.HexColor('#fafafa'),colors.HexColor('#f5f5f5')]),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),('TOPPADDING',(0,0),(-1,-1),3),('BOTTOMPADDING',(0,0),(-1,-1),3)]))
    story+=[t4,PageBreak()]

    # 第九章
    story+=[Paragraph('九、安全机制',st['H1']),HRFlowable(width="100%",thickness=1,color=colors.HexColor('#e0e0e0'))]
    story+=[Paragraph('flink-yarn 模块对 Kerberos 安全和 Delegation Token 有完善的支持:',st['B'])]
    for d in ['Keytab 分发: 支持 ship-local-keytab(上传到HDFS) 和 pre-installed(预装在节点上) 两种方式',
              'Delegation Token: 通过 DefaultDelegationTokenManager 获取 HDFS/Hive/HBase 等服务的 token',
              'AM Token 注入: setTokensFor() 将所有 token 序列化到 ContainerLaunchContext',
              'TM Token 传递: TaskExecutor 从 HADOOP_TOKEN_FILE_LOCATION 读取 token 文件，过滤掉 AMRMToken',
              'KRB5 配置: 支持将 krb5.conf 作为 LocalResource 分发到容器',
              '安全模式检测: HadoopUtils.isKerberosSecurityEnabled() 检查是否启用 Kerberos',
              'SecurityUtils.runSecured(): CLI 在安全上下文中执行所有操作']:
        story+=[Paragraph(f'  \u2022 {d}',st['BL'])]
    story+=[PageBreak()]

    # 第十章
    story+=[Paragraph('十、关键设计模式与架构思想',st['H1']),HRFlowable(width="100%",thickness=1,color=colors.HexColor('#e0e0e0'))]
    patterns=[
        ('工厂模式','YarnClusterClientFactory / YarnResourceManagerFactory / DefaultYarnResourceManagerClientFactory — 所有核心组件通过工厂创建，解耦实例化逻辑'),
        ('模板方法','AbstractResourceManagerDriver.initializeInternal() 由子类 YarnResourceManagerDriver 实现；AbstractYarnCli 定义 CLI 框架'),
        ('策略模式','UserJarInclusion (FIRST/ORDER/LAST/DISABLED) 控制classpath构建策略'),
        ('回调模式','AMRMCallbackHandler / NMCallbackHandler — YARN 异步客户端的事件处理'),
        ('反射适配','ApplicationSubmissionContextReflector / ContainerRequestReflector / ResourceInformationReflector — 兼容不同Hadoop版本'),
        ('单例模式','YarnResourceManagerFactory / AMRMClientAsyncReflector / ContainerRequestReflector / ResourceInformationReflector'),
        ('CompletableFuture异步编排','requestResource() 返回 Future，容器分配后 complete()，支持取消(CancellationException)'),
        ('ShutdownHook保护','DeploymentFailureHook 防止部署失败时资源泄漏；FlinkYarnSessionCli 关闭时清理集群'),
        ('Phaser并发协调','trackerOfReleasedResources 使用 Phaser 而非 CountDownLatch，支持动态注册/注销'),
        ('SPI 服务发现','YarnSessionClusterExecutorFactory 通过 META-INF/services 注册，实现插件化部署目标'),
    ]
    for name,desc in patterns:
        story+=[Paragraph(f'<b>{name}</b>',st['H3'])]
        story+=[Paragraph(desc,st['BI'])]

    story+=[Spacer(1,30),HRFlowable(width="100%",thickness=2,color=colors.HexColor('#1565c0'))]
    story+=[Paragraph('文档结束 — 基于 Flink 1.15.4 flink-yarn 模块全部源码分析生成',st['Cap'])]

    doc.build(story)
    print(f"PDF generated: {out}")
    return out

if __name__ == '__main__':
    build()
