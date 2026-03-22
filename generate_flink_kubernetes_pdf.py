#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Flink Kubernetes 模块源码分析 - PDF 生成脚本"""

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
    a('CodeBlock', fontName='Courier', fontSize=8, leading=11, spaceBefore=2, spaceAfter=2, leftIndent=20, textColor=colors.HexColor('#37474f'))
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

def dashedline(d,x1,y1,x2,y2,sc=colors.HexColor('#90a4ae'),sw=0.8):
    d.add(Line(x1,y1,x2,y2,strokeColor=sc,strokeWidth=sw,strokeDashArray=[4,3]))

# === 图表 ===

def fig_architecture():
    """总体架构概览图"""
    d=Drawing(500,520)
    d.add(Rect(0,0,500,520,fillColor=colors.HexColor('#fafafa'),strokeColor=colors.HexColor('#e0e0e0'),strokeWidth=0.5))
    label(d,250,500,'Flink on Kubernetes 整体架构',13,colors.HexColor('#1565c0'))

    # CLI / Client 层
    label(d,250,478,'CLI / Client',10,colors.HexColor('#e65100'))
    box(d,30,445,120,28,'KubernetesSessionCli',colors.HexColor('#e65100'),fs=8)
    box(d,180,445,160,28,'KubernetesClusterClientFactory',colors.HexColor('#f57c00'),fs=7)
    box(d,370,445,120,28,'SessionClusterExecutor',colors.HexColor('#ff9800'),fs=7)

    # Cluster Descriptor 层
    label(d,250,425,'Cluster Descriptor',10,colors.HexColor('#1565c0'))
    box(d,130,390,240,28,'KubernetesClusterDescriptor',colors.HexColor('#1565c0'),fs=9)
    arrow(d,250,445,250,418)

    # Factory 层
    label(d,250,370,'Factory (Decorator Pattern)',10,colors.HexColor('#2e7d32'))
    box(d,30,335,190,28,'KubernetesJobManagerFactory',colors.HexColor('#2e7d32'),fs=8)
    box(d,260,335,200,28,'KubernetesTaskManagerFactory',colors.HexColor('#388e3c'),fs=8)
    arrow(d,200,390,150,363)
    arrow(d,300,390,360,363)

    # Decorator 层
    label(d,250,315,'Step Decorators',10,colors.HexColor('#00695c'))
    decorators = ['Init*', 'Cmd*', 'FlinkConf\nMount', 'External\nService', 'Internal\nService',
                  'Hadoop\nConf', 'Kerberos', 'Secrets', 'PVC\nMount', 'PodTpl\nMount']
    for i, dec in enumerate(decorators):
        xp = 10 + i * 49
        box(d, xp, 265, 46, 40, dec, colors.HexColor('#00897b'), fs=6)
    arrow(d, 250, 335, 250, 305)

    # K8s Client 层
    label(d,250,245,'Kubernetes Client',10,colors.HexColor('#6a1b9a'))
    box(d,60,210,170,28,'FlinkKubeClient (Interface)',colors.HexColor('#e1bee7'),colors.HexColor('#6a1b9a'),fs=8)
    box(d,270,210,170,28,'Fabric8FlinkKubeClient',colors.HexColor('#6a1b9a'),fs=8)
    arrow(d,230,224,270,224)
    arrow(d,250,265,250,238)

    # Entrypoint 层 (JM 进程内)
    dashedline(d,10,195,490,195)
    label(d,250,180,'JM Pod (Entrypoint)',10,colors.HexColor('#c62828'))
    box(d,20,145,150,28,'SessionCluster\nEntrypoint',colors.HexColor('#c62828'),fs=7)
    box(d,190,145,150,28,'ApplicationCluster\nEntrypoint',colors.HexColor('#d32f2f'),fs=7)
    box(d,360,145,130,28,'EntrypointUtils',colors.HexColor('#e53935'),fs=8)

    # Resource Manager 层
    label(d,250,125,'Resource Manager',10,colors.HexColor('#0d47a1'))
    box(d,100,90,300,28,'KubernetesResourceManagerDriver',colors.HexColor('#0d47a1'),fs=9)
    arrow(d,170,145,200,118)
    arrow(d,265,145,250,118)

    # HA 层
    label(d,250,70,'High Availability (ConfigMap-based)',10,colors.HexColor('#4a148c'))
    box(d,20,30,150,30,'LeaderElection\nHaServices',colors.HexColor('#4a148c'),fs=7)
    box(d,190,30,150,30,'StateHandleStore\n(ConfigMap)',colors.HexColor('#6a1b9a'),fs=7)
    box(d,360,30,130,30,'LeaderElection\nDriver',colors.HexColor('#7b1fa2'),fs=7)

    return d


def fig_deploy_session():
    """Session 集群部署流程图"""
    d=Drawing(500,600)
    d.add(Rect(0,0,500,600,fillColor=colors.HexColor('#fafafa'),strokeColor=colors.HexColor('#e0e0e0'),strokeWidth=0.5))
    label(d,250,580,'Session 集群部署核心流程',13,colors.HexColor('#1565c0'))

    bw,bh,cx=170,30,165
    steps=[
        ('1. CLI 解析参数','#1565c0'),
        ('2. 创建 ClusterDescriptor','#1976d2'),
        ('3. deploySessionCluster()','#1e88e5'),
        ('4. 构建 JM Parameters','#2196f3'),
        ('5. 加载 Pod 模板(可选)','#42a5f5'),
        ('6. 装饰器链构建 JM Spec','#1e88e5'),
        ('7. createJobManagerComponent()','#1976d2'),
        ('8. 创建 Deployment + Service','#1565c0'),
        ('9. 等待 REST 端点就绪','#0d47a1'),
        ('10. 返回 RestClusterClient','#002171'),
    ]
    notes = [
        'KubernetesSessionCli',
        'KubernetesClusterClientFactory',
        'entrypoint=Session',
        'KubernetesJobManagerParameters',
        'loadPodFromTemplateFile()',
        'KubernetesJobManagerFactory',
        'Fabric8FlinkKubeClient',
        'K8s API (Fabric8)',
        'getRestEndpoint()',
        'RestClusterClient<String>',
    ]
    for i,(s,c) in enumerate(steps):
        y=530-i*52
        box(d,cx,y,bw,bh,s,colors.HexColor(c),fs=8)
        if i>0: arrow(d,cx+bw/2,y+52,cx+bw/2,y+bh)
        box(d,360,y+2,120,26,notes[i],colors.HexColor('#e3f2fd'),colors.HexColor('#1565c0'),6)
        arrow(d,cx+bw,y+bh/2,360,y+15,colors.HexColor('#90caf9'),0.8)
    return d


def fig_deploy_application():
    """Application 集群部署流程图"""
    d=Drawing(500,520)
    d.add(Rect(0,0,500,520,fillColor=colors.HexColor('#fafafa'),strokeColor=colors.HexColor('#e0e0e0'),strokeWidth=0.5))
    label(d,250,500,'Application 集群部署流程',13,colors.HexColor('#1565c0'))

    bw,bh,cx=170,30,165
    steps=[
        ('1. deployApplicationCluster()','#e65100'),
        ('2. 上传用户 JAR (Artifact)','#f57c00'),
        ('3. 构建 JM Parameters','#fb8c00'),
        ('4. 装饰器链构建 JM Spec','#ff9800'),
        ('5. createJobManagerComponent()','#f57c00'),
        ('6. AM Pod 启动 main()','#e65100'),
        ('7. 生成 JobGraph','#d84315'),
        ('8. ResourceManager 请求 TM','#bf360c'),
    ]
    notes = [
        'entrypoint=Application',
        'KubernetesArtifactUploader',
        'KubernetesJobManagerParams',
        'KubernetesJobManagerFactory',
        'Fabric8FlinkKubeClient',
        'ApplicationClusterEntrypoint',
        'PackagedProgram.invokeMain()',
        'KubernetesRMDriver',
    ]
    for i,(s,c) in enumerate(steps):
        y=450-i*52
        box(d,cx,y,bw,bh,s,colors.HexColor(c),fs=8)
        if i>0: arrow(d,cx+bw/2,y+52,cx+bw/2,y+bh)
        box(d,360,y+2,120,26,notes[i],colors.HexColor('#fff3e0'),colors.HexColor('#e65100'),6)
        arrow(d,cx+bw,y+bh/2,360,y+15,colors.HexColor('#ffcc80'),0.8)
    return d


def fig_resource_management():
    """资源管理(TM Pod 创建)流程图"""
    d=Drawing(500,480)
    d.add(Rect(0,0,500,480,fillColor=colors.HexColor('#fafafa'),strokeColor=colors.HexColor('#e0e0e0'),strokeWidth=0.5))
    label(d,250,460,'TM Pod 资源管理核心流程',13,colors.HexColor('#1565c0'))

    # 三列: Flink RM | KubernetesResourceManagerDriver | K8s API
    label(d,70,435,'SlotManager',10,colors.HexColor('#e65100'))
    label(d,250,435,'KubernetesRMDriver',10,colors.HexColor('#1565c0'))
    label(d,430,435,'K8s API (Fabric8)',10,colors.HexColor('#2e7d32'))

    # 竖线
    for x in [70,250,430]:
        d.add(Line(x,420,x,30,strokeColor=colors.HexColor('#e0e0e0'),strokeWidth=0.8))

    # 步骤 (时序图风格)
    y = 400
    # 1. requestResource
    box(d,15,y,110,22,'requestResource()',colors.HexColor('#e65100'),fs=7)
    arrow(d,125,y+11,190,y+11)
    box(d,190,y,120,22,'构建 TM Parameters',colors.HexColor('#1565c0'),fs=7)
    y -= 40

    # 2. buildTaskManagerPod
    box(d,190,y,120,22,'装饰器链构建 Pod',colors.HexColor('#1976d2'),fs=7)
    arrow(d,310,y+11,375,y+11)
    box(d,375,y,110,22,'createTaskMgrPod',colors.HexColor('#2e7d32'),fs=7)
    y -= 40

    # 3. Pod Created
    box(d,375,y,110,22,'Pod ADDED 事件',colors.HexColor('#388e3c'),fs=7)
    arrow(d,375,y+11,310,y+11,colors.HexColor('#66bb6a'))
    box(d,190,y,120,22,'PodCallbackHandler',colors.HexColor('#1e88e5'),fs=7)
    y -= 40

    # 4. onAdded
    box(d,190,y,120,22,'onAdded: 完成 Future',colors.HexColor('#2196f3'),fs=7)
    arrow(d,190,y+11,125,y+11,colors.HexColor('#42a5f5'))
    box(d,15,y,110,22,'分配 Slot',colors.HexColor('#f57c00'),fs=7)
    y -= 40

    # 5. Pod Running
    box(d,375,y,110,22,'Pod RUNNING',colors.HexColor('#43a047'),fs=7)
    arrow(d,375,y+11,310,y+11,colors.HexColor('#66bb6a'))
    box(d,190,y,120,22,'onModified',colors.HexColor('#42a5f5'),fs=7)
    y -= 40

    # 6. TaskExecutor 注册
    box(d,15,y,110,22,'TM 注册到 RM',colors.HexColor('#ff9800'),fs=7)
    arrow(d,125,y+11,190,y+11,colors.HexColor('#ffb74d'))
    box(d,190,y,120,22,'onRegistration',colors.HexColor('#1565c0'),fs=7)
    y -= 50

    # 7. 释放流程
    dashedline(d,10,y+35,490,y+35)
    label(d,250,y+22,'释放流程',9,colors.HexColor('#c62828'))
    box(d,15,y,110,22,'releaseResource()',colors.HexColor('#c62828'),fs=7)
    arrow(d,125,y+11,190,y+11,colors.HexColor('#ef5350'))
    box(d,190,y,120,22,'stopPod(podName)',colors.HexColor('#d32f2f'),fs=7)
    arrow(d,310,y+11,375,y+11,colors.HexColor('#ef5350'))
    box(d,375,y,110,22,'DELETE Pod',colors.HexColor('#b71c1c'),fs=7)
    y -= 40

    # 8. onDeleted
    box(d,375,y,110,22,'Pod DELETED 事件',colors.HexColor('#c62828'),fs=7)
    arrow(d,375,y+11,310,y+11,colors.HexColor('#ef5350'))
    box(d,190,y,120,22,'onDeleted: 回收资源',colors.HexColor('#d32f2f'),fs=7)

    # Pod 命名说明
    label(d,250,20,'Pod 命名: {clusterId}-taskmanager-{attemptId}-{podIndex}',8,colors.HexColor('#9e9e9e'))

    return d


def fig_decorator_chain():
    """装饰器链式构建流程图"""
    d=Drawing(500,560)
    d.add(Rect(0,0,500,560,fillColor=colors.HexColor('#fafafa'),strokeColor=colors.HexColor('#e0e0e0'),strokeWidth=0.5))
    label(d,250,540,'装饰器链式构建 JM Pod 流程',13,colors.HexColor('#1565c0'))

    # FlinkPod 输入
    box(d,20,490,100,30,'FlinkPod\n(空或模板)',colors.HexColor('#e3f2fd'),colors.HexColor('#1565c0'),8)
    arrow(d,120,505,160,505)

    # 装饰器列表
    decorators = [
        ('1. InitJobManagerDecorator', '#1b5e20', '设置镜像、CPU/Memory、端口、\n标签、注解、Tolerations'),
        ('2. EnvSecretsDecorator', '#2e7d32', '通过 K8s Secret 注入环境变量'),
        ('3. MountSecretsDecorator', '#388e3c', '挂载 K8s Secret 为 Volume'),
        ('4. PVCMountDecorator', '#43a047', '挂载 PersistentVolumeClaim'),
        ('5. CmdJobManagerDecorator', '#4caf50', '设置 JM 启动命令和参数'),
        ('6. InternalServiceDecorator', '#66bb6a', '创建 Headless Service (非HA)'),
        ('7. ExternalServiceDecorator', '#81c784', '创建 REST Service\n(ClusterIP/NodePort/LB)'),
        ('8. HadoopConfMountDecorator', '#a5d6a7', '挂载 Hadoop 配置 ConfigMap'),
        ('9. KerberosMountDecorator', '#c8e6c9', '挂载 Kerberos keytab'),
        ('10. FlinkConfMountDecorator', '#2e7d32', '挂载 config.yaml + 日志配置'),
        ('11. PodTemplateMountDecorator', '#1b5e20', '将 TM Pod 模板挂载到 JM'),
    ]
    for i,(name,clr,desc) in enumerate(decorators):
        y = 480 - i * 40
        box(d, 160, y, 155, 30, name, colors.HexColor(clr), fs=7)
        box(d, 330, y + 2, 160, 26, desc, colors.HexColor('#e8f5e9'), colors.HexColor('#1b5e20'), 6)
        arrow(d, 315, y + 15, 330, y + 15, colors.HexColor('#81c784'), 0.8)
        if i > 0:
            arrow(d, 237, y + 40, 237, y + 30)

    # 输出
    y_out = 480 - len(decorators) * 40
    arrow(d, 237, y_out + 40, 237, y_out + 20)
    box(d, 160, y_out - 15, 155, 30, 'KubernetesJMSpecification', colors.HexColor('#0d47a1'), fs=7)
    label(d, 237, y_out - 25, 'Deployment + Service + ConfigMap', 7, colors.HexColor('#455a64'))

    return d


def fig_modes_comparison():
    """Session 模式 vs Application 模式对比图"""
    d=Drawing(500,380)
    d.add(Rect(0,0,500,380,fillColor=colors.HexColor('#fafafa'),strokeColor=colors.HexColor('#e0e0e0'),strokeWidth=0.5))
    label(d,250,360,'Session 模式 vs Application 模式',13,colors.HexColor('#1565c0'))

    # Session
    label(d,120,335,'Session 模式',10,colors.HexColor('#e65100'))
    box(d,20,300,90,26,'Flink Client',colors.HexColor('#e65100'),fs=8)
    box(d,140,300,100,26,'Session Cluster',colors.HexColor('#f57c00'),fs=8)
    arrow(d,110,313,140,313)
    box(d,140,265,100,26,'JM (常驻运行)',colors.HexColor('#fb8c00'),fs=8)
    box(d,140,230,100,26,'TMs (共享池)',colors.HexColor('#ff9800'),fs=8)
    arrow(d,190,300,190,291,colors.HexColor('#ffb74d'))
    arrow(d,190,265,190,256,colors.HexColor('#ffb74d'))
    label(d,120,215,'多 Job 共享同一集群资源',7,colors.HexColor('#9e9e9e'))

    # Application
    label(d,380,335,'Application 模式',10,colors.HexColor('#1565c0'))
    box(d,290,300,90,26,'Flink Client',colors.HexColor('#1565c0'),fs=8)
    box(d,400,300,90,26,'App Cluster',colors.HexColor('#1976d2'),fs=8)
    arrow(d,380,313,400,313)
    box(d,400,265,90,26,'AM + main()',colors.HexColor('#2196f3'),fs=8)
    box(d,400,230,90,26,'独立 TMs',colors.HexColor('#42a5f5'),fs=8)
    arrow(d,445,300,445,291,colors.HexColor('#64b5f6'))
    arrow(d,445,265,445,256,colors.HexColor('#64b5f6'))
    label(d,380,215,'Job 级别完全资源隔离',7,colors.HexColor('#9e9e9e'))

    # 对比表
    label(d,250,185,'关键差异对比',10,colors.HexColor('#455a64'))
    d.add(Rect(20,10,460,165,fillColor=colors.HexColor('#eceff1'),strokeColor=colors.HexColor('#b0bec5'),strokeWidth=0.5,rx=3,ry=3))
    items=[
        ('入口点','KubernetesSession\nClusterEntrypoint','KubernetesApplication\nClusterEntrypoint'),
        ('JobGraph','客户端生成并通过\nREST API提交','AM Pod 内执行\nmain() 生成'),
        ('资源隔离','共享集群, 无隔离','独立集群, 完全隔离'),
        ('Entrypoint类','SessionClusterEntrypoint','ApplicationClusterEntryPoint'),
        ('用户JAR','通过REST上传','打入容器镜像或\nArtifact上传'),
        ('适用场景','交互式开发 / 多短Job','生产环境 / 大型Job'),
    ]
    for i,(l,s,a) in enumerate(items):
        yy=155-i*23
        label(d,60,yy,l,7,colors.HexColor('#455a64'))
        label(d,190,yy,s,6,colors.HexColor('#e65100'))
        label(d,390,yy,a,6,colors.HexColor('#1565c0'))
    return d


def fig_ha_leader_election():
    """基于 ConfigMap 的 HA Leader 选举时序图"""
    d=Drawing(500,440)
    d.add(Rect(0,0,500,440,fillColor=colors.HexColor('#fafafa'),strokeColor=colors.HexColor('#e0e0e0'),strokeWidth=0.5))
    label(d,250,420,'基于 ConfigMap 的 HA Leader 选举流程',13,colors.HexColor('#1565c0'))

    # 三个参与者
    participants = [
        (80, 'JM Candidate', '#4a148c'),
        (250, 'K8s ConfigMap', '#1565c0'),
        (420, 'ConfigMap Watcher', '#2e7d32'),
    ]
    for x, name, c in participants:
        box(d, x-55, 385, 110, 24, name, colors.HexColor(c), fs=8)
        d.add(Line(x, 385, x, 30, strokeColor=colors.HexColor('#e0e0e0'), strokeWidth=0.8))

    y = 365
    step_h = 40

    # 1. 创建 LeaderElector
    box(d, 25, y, 110, 20, '创建 LeaderElector', colors.HexColor('#7b1fa2'), fs=7)
    arrow(d, 135, y+10, 195, y+10)
    box(d, 195, y, 110, 20, '创建/更新 ConfigMap\n(写入 leader 信息)', colors.HexColor('#1976d2'), fs=6)
    y -= step_h

    # 2. 选举成功
    arrow(d, 195, y+10, 135, y+10, colors.HexColor('#7b1fa2'))
    box(d, 25, y, 110, 20, 'isLeader() 回调', colors.HexColor('#6a1b9a'), fs=7)
    y -= step_h

    # 3. Watcher 感知
    arrow(d, 305, y+10, 365, y+10, colors.HexColor('#2e7d32'))
    box(d, 365, y, 110, 20, 'onModified 事件', colors.HexColor('#388e3c'), fs=7)
    y -= step_h

    # 4. 通知 LeaderRetrievalService
    box(d, 365, y, 110, 20, '通知 Retrieval\nService', colors.HexColor('#43a047'), fs=7)
    y -= step_h

    # 5. RM/Dispatcher 获取 leader
    box(d, 365, y, 110, 20, '更新 leader 地址', colors.HexColor('#66bb6a'), fs=7)
    y -= step_h

    # Failover 场景
    dashedline(d, 10, y+25, 490, y+25)
    label(d, 250, y+12, 'Leader 故障 (Failover)', 9, colors.HexColor('#c62828'))

    # 6. Leader 丢失
    box(d, 25, y, 110, 20, 'notLeader() 回调', colors.HexColor('#c62828'), fs=7)
    arrow(d, 135, y+10, 195, y+10, colors.HexColor('#ef5350'))
    box(d, 195, y, 110, 20, 'ConfigMap 更新\n(leader 信息清除)', colors.HexColor('#d32f2f'), fs=6)
    y -= step_h

    # 7. 新 Leader 竞选
    box(d, 25, y, 110, 20, '新 Candidate 竞选', colors.HexColor('#7b1fa2'), fs=7)
    arrow(d, 135, y+10, 195, y+10)
    box(d, 195, y, 110, 20, 'ConfigMap CAS 更新\n(乐观锁)', colors.HexColor('#1976d2'), fs=6)
    y -= step_h

    # 8. 状态恢复
    box(d, 195, y, 110, 20, 'StateHandleStore\n读取状态', colors.HexColor('#0d47a1'), fs=6)
    arrow(d, 195, y+10, 135, y+10, colors.HexColor('#42a5f5'))
    box(d, 25, y, 110, 20, '恢复 Checkpoint\n状态', colors.HexColor('#4a148c'), fs=7)

    return d


def fig_service_types():
    """K8s Service 类型策略图"""
    d=Drawing(500,300)
    d.add(Rect(0,0,500,300,fillColor=colors.HexColor('#fafafa'),strokeColor=colors.HexColor('#e0e0e0'),strokeWidth=0.5))
    label(d,250,280,'Kubernetes Service 暴露策略',13,colors.HexColor('#1565c0'))

    # 抽象基类
    box(d,175,235,150,30,'ServiceType\n(Abstract)',colors.HexColor('#1565c0'),fs=9)

    # 四种实现
    types = [
        (15, 'ClusterIPService', '#2e7d32', 'type: ClusterIP\n集群内部访问'),
        (135, 'HeadlessClusterIP\nService', '#388e3c', 'clusterIP: None\n直接Pod IP'),
        (260, 'NodePortService', '#e65100', 'type: NodePort\n节点端口映射'),
        (380, 'LoadBalancerService', '#c62828', 'type: LoadBalancer\n外部负载均衡'),
    ]
    for x, name, clr, desc in types:
        box(d, x, 175, 110, 35, name, colors.HexColor(clr), fs=7)
        arrow(d, x+55, 235, x+55, 210)
        box(d, x, 120, 110, 40, desc, colors.HexColor('#f5f5f5'), colors.HexColor('#455a64'), 6)

    # 使用场景
    label(d, 250, 95, '配置项: kubernetes.rest-service.exposed.type', 8, colors.HexColor('#9e9e9e'))

    # 方法列表
    d.add(Rect(30,20,440,60,fillColor=colors.HexColor('#eceff1'),strokeColor=colors.HexColor('#b0bec5'),strokeWidth=0.5,rx=3,ry=3))
    label(d,250,65,'核心方法',8,colors.HexColor('#455a64'))
    methods = ['buildUpExternalRestService()', 'buildUpInternalService()', 'getRestEndpoint()', 'getType() / getRestPort()']
    for i, m in enumerate(methods):
        label(d, 80 + i*110, 40, m, 6, colors.HexColor('#1565c0'))

    return d


def fig_class_hierarchy():
    """核心类层次结构图"""
    d=Drawing(500,480)
    d.add(Rect(0,0,500,480,fillColor=colors.HexColor('#fafafa'),strokeColor=colors.HexColor('#e0e0e0'),strokeWidth=0.5))
    label(d,250,460,'核心类层次结构',13,colors.HexColor('#1565c0'))

    bw=175
    layers=[
        ('集群部署层','#e65100',[('ClusterDescriptor<String>','#fff3e0','#e65100'),('KubernetesClusterDescriptor','#e65100','#ffffff')]),
        ('入口点层','#2e7d32',[('ClusterEntrypoint','#e8f5e9','#2e7d32'),('K8sSession/AppEntryPoint','#2e7d32','#ffffff')]),
        ('资源管理层','#1565c0',[('AbstractRMDriver<T>','#e3f2fd','#1565c0'),('KubernetesRMDriver','#1565c0','#ffffff')]),
        ('K8s客户端层','#6a1b9a',[('FlinkKubeClient (接口)','#f3e5f5','#6a1b9a'),('Fabric8FlinkKubeClient','#6a1b9a','#ffffff')]),
        ('参数配置层','#00695c',[('KubernetesParameters','#e0f2f1','#00695c'),('JM/TM Parameters','#00695c','#ffffff')]),
        ('装饰器层','#c62828',[('KubernetesStepDecorator','#ffebee','#c62828'),('Init/Cmd/Mount/Service...','#c62828','#ffffff')]),
    ]
    for i,(lbl,lc,classes) in enumerate(layers):
        y=420-i*65
        label(d,60,y+10,lbl,10,colors.HexColor(lc))
        for j,(cn,bg,tc) in enumerate(classes):
            box(d,10+j*200,y-25,bw,24,cn,colors.HexColor(bg),colors.HexColor(tc),8)
        if len(classes)==2:
            arrow(d,10+bw,y-13,210,y-13)
    return d


# === 构建PDF ===
def build():
    out=os.path.join(os.path.dirname(os.path.abspath(__file__)),'Flink_Kubernetes_模块源码分析.pdf')
    doc=SimpleDocTemplate(out,pagesize=A4,rightMargin=2*cm,leftMargin=2*cm,topMargin=2.5*cm,bottomMargin=2*cm)
    st=mkstyles()
    story=[]

    # ==================== 封面 ====================
    story+=[Spacer(1,80),Paragraph('Flink Kubernetes 模块',st['T']),Paragraph('核心流程与设计说明',st['T']),
            Spacer(1,20),HRFlowable(width="60%",thickness=2,color=colors.HexColor('#1565c0')),Spacer(1,20),
            Paragraph('基于 Apache Flink 1.15.4 Release 分支源码分析',st['ST']),
            Paragraph('flink-kubernetes 模块完整解读',st['ST']),Spacer(1,40),
            Paragraph('模块路径: flink-kubernetes/src/main/java/org/apache/flink/kubernetes/',st['ST']),
            Paragraph('共 90+ 个核心 Java 源文件 | 154 个 Java 文件(含测试)',st['ST']),PageBreak()]

    # ==================== 目录 ====================
    story+=[Paragraph('目录',st['H1']),Spacer(1,10)]
    for t in ['一、模块概览与架构设计','二、核心类设计说明','三、Session 集群部署流程',
              '四、Application 集群部署流程','五、TM Pod 资源管理流程',
              '六、装饰器模式与 Pod 构建','七、Session vs Application 模式对比',
              '八、K8s Service 暴露策略','九、高可用 (HA) 机制',
              '十、参数与配置体系','十一、Pod 模板支持','十二、关键设计模式']:
        story+=[Paragraph(t,st['B']),Spacer(1,3)]
    story+=[PageBreak()]

    # ==================== 第一章 ====================
    story+=[Paragraph('一、模块概览与架构设计',st['H1']),HRFlowable(width="100%",thickness=1,color=colors.HexColor('#e0e0e0'))]
    story+=[Paragraph('1.1 模块定位',st['H2'])]
    story+=[Paragraph('flink-kubernetes 模块是 Apache Flink 与 Kubernetes 集成的核心桥梁。负责将 Flink 集群(Session/Application)部署到 K8s，通过 K8s API 管理 JobManager Deployment 和 TaskManager Pod 的完整生命周期，支持 K8s 原生的服务发现、配置管理和高可用。底层基于 Fabric8 Kubernetes Client 库实现。',st['B'])]

    story+=[Paragraph('1.2 模块包结构',st['H2'])]
    t=Table([['包名','职责','核心类数'],
             ['o.a.f.kubernetes','集群描述符、资源管理驱动','4'],
             ['o.a.f.kubernetes.kubeclient','K8s客户端接口/实现、FlinkPod抽象','8+'],
             ['o.a.f.kubernetes.kubeclient.factory','JM/TM Pod工厂','2'],
             ['o.a.f.kubernetes.kubeclient.decorators','装饰器: Pod增强','15'],
             ['o.a.f.kubernetes.kubeclient.parameters','JM/TM参数体系','4'],
             ['o.a.f.kubernetes.kubeclient.resources','K8s资源封装','15'],
             ['o.a.f.kubernetes.kubeclient.services','Service类型策略','5'],
             ['o.a.f.kubernetes.configuration','K8s配置项定义','6'],
             ['o.a.f.kubernetes.entrypoint','JM进程入口点','5'],
             ['o.a.f.kubernetes.highavailability','基于ConfigMap的HA','12'],
             ['o.a.f.kubernetes.cli','Session CLI','1'],
             ['o.a.f.kubernetes.utils','常量和工具类','2']],colWidths=[150,200,60])
    t.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),colors.HexColor('#1565c0')),('TEXTCOLOR',(0,0),(-1,0),colors.white),
        ('FONTNAME',(0,0),(-1,-1),FONT_CN),('FONTSIZE',(0,0),(-1,-1),8),('ALIGN',(0,0),(-1,-1),'CENTER'),
        ('GRID',(0,0),(-1,-1),0.5,colors.HexColor('#b0bec5')),('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.HexColor('#fafafa'),colors.HexColor('#f5f5f5')]),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),('TOPPADDING',(0,0),(-1,-1),4),('BOTTOMPADDING',(0,0),(-1,-1),4)]))
    story+=[t,Spacer(1,10)]

    story+=[Paragraph('1.3 核心类层次结构',st['H2'])]
    story+=[fig_class_hierarchy(),Paragraph('图 1-1: 核心类层次结构',st['Cap'])]

    story+=[Paragraph('1.4 整体架构概览',st['H2'])]
    story+=[fig_architecture(),Paragraph('图 1-2: Flink on Kubernetes 整体架构',st['Cap']),PageBreak()]

    # ==================== 第二章 ====================
    story+=[Paragraph('二、核心类设计说明',st['H1']),HRFlowable(width="100%",thickness=1,color=colors.HexColor('#e0e0e0'))]

    # 2.1 KubernetesClusterDescriptor
    story+=[Paragraph('2.1 KubernetesClusterDescriptor (约331行)',st['H2'])]
    story+=[Paragraph('实现 ClusterDescriptor&lt;String&gt; 接口(泛型参数为 clusterId 字符串)，是集群部署的核心协调者。',st['B'])]
    for r in [
        'deploySessionCluster(): 设置 entrypoint 为 KubernetesSessionClusterEntrypoint，调用 deployClusterInternal()',
        'deployApplicationCluster(): 设置 entrypoint 为 KubernetesApplicationClusterEntrypoint，可选上传用户 JAR (Artifact)',
        'deployClusterInternal(): 核心部署逻辑 — 构建 KubernetesJobManagerParameters -> 加载 Pod 模板 -> KubernetesJobManagerFactory 构建 JM Spec -> client.createJobManagerComponent()',
        'retrieve(clusterId): 根据 clusterId 获取已有集群的 RestClusterClient',
        'killCluster(clusterId): 清理集群全部 K8s 资源',
        'getClusterConnectionInfo(): 通过 REST Service 端点获取集群连接信息',
        '核心成员: FlinkKubeClient client / FlinkKubeClientFactory clientFactory / KubernetesArtifactUploader artifactUploader',
    ]:
        story+=[Paragraph(f'  \u2022 {r}',st['BL'])]

    # 2.2 KubernetesResourceManagerDriver
    story+=[Paragraph('2.2 KubernetesResourceManagerDriver (约509行)',st['H2'])]
    story+=[Paragraph('继承 AbstractResourceManagerDriver&lt;KubernetesWorkerNode&gt;，运行在 JM Pod 中，负责 TM Pod 的动态创建/销毁。',st['B'])]
    for r in [
        'initializeInternal(): 初始化 — Watch TM Pods + 加载 Pod 模板 + 恢复之前的 Worker 节点',
        'requestResource(): 创建 TM Pod: 构建 KubernetesTaskManagerParameters -> 装饰器链构建 Pod -> flinkKubeClient.createTaskManagerPod()',
        'releaseResource(): 停止 TM Pod (调用 flinkKubeClient.stopPod())',
        'recoverWorkerNodesFromPreviousAttempts(): RM Failover 后恢复已有的 TM Pod',
        'PodCallbackHandlerImpl 内部类: 处理 Pod Watch 事件 — onAdded(完成 Future) / onModified / onDeleted(回收资源) / onError',
        'Pod 命名规则: {clusterId}-taskmanager-{attemptId}-{podIndex}',
        'requestResourceFutures: 异步资源请求 Future 缓存，容器创建后 complete()',
        'currentMaxAttemptId / currentMaxPodId: Pod 命名计数器，确保唯一性',
    ]:
        story+=[Paragraph(f'  \u2022 {r}',st['BL'])]

    # 2.3 Fabric8FlinkKubeClient
    story+=[Paragraph('2.3 Fabric8FlinkKubeClient (约430行)',st['H2'])]
    story+=[Paragraph('基于 Fabric8 Kubernetes Client 实现 FlinkKubeClient 接口，封装所有 K8s API 操作。',st['B'])]
    for r in [
        'createJobManagerComponent(): 创建 Deployment + 设置 OwnerReference + 创建附属资源(Service/ConfigMap)',
        'createTaskManagerPod(): 异步创建 TM Pod，设置 OwnerReference 关联到主 Deployment(级联删除)',
        'stopPod() / stopAndCleanupCluster(): 资源清理',
        'getRestEndpoint(): 获取 REST 服务端点(支持 ClusterIP/NodePort/LoadBalancer)',
        'watchPodsAndDoCallback(): Watch Pod 事件，带指数退避重试机制',
        'checkAndUpdateConfigMap(): 乐观锁事务更新 ConfigMap(用于 HA Leader 选举)',
        'loadPodFromTemplateFile(): 从 YAML 文件加载 Pod 模板',
        'kubeClientExecutorService: 专用异步 IO 线程池(默认4线程)',
        'masterDeploymentRef: 主 Deployment 引用(AtomicReference)，用于设置 OwnerReference',
    ]:
        story+=[Paragraph(f'  \u2022 {r}',st['BL'])]

    # 2.4 FlinkPod
    story+=[Paragraph('2.4 FlinkPod 抽象 (约96行)',st['H2'])]
    story+=[Paragraph('核心 Pod 抽象，将 Pod 分为两个独立部分，使装饰器可以分别操作 Pod-level 和 Container-level 配置:',st['B'])]
    for r in [
        'podWithoutMainContainer: Pod spec (不含主容器)，用于设置 Volume、NodeSelector、Tolerations 等 Pod 级配置',
        'mainContainer: 主容器(名为 flink-main-container)，用于设置镜像、资源、端口、环境变量、VolumeMount 等',
        'Builder 模式: 提供链式构建 API，copy() 方法支持不可变拷贝',
        '设计意义: 装饰器可独立修改 Pod 或 Container，避免修改冲突',
    ]:
        story+=[Paragraph(f'  \u2022 {r}',st['BL'])]

    # 2.5 KubernetesJobManagerFactory / KubernetesTaskManagerFactory
    story+=[Paragraph('2.5 KubernetesJobManagerFactory / TaskManagerFactory',st['H2'])]
    story+=[Paragraph('静态工厂，通过装饰器链式调用构建 JM Deployment 和 TM Pod:',st['B'])]
    for r in [
        'KubernetesJobManagerFactory.buildKubernetesJobManagerSpecification(): 按顺序执行 11 个装饰器，将 FlinkPod 封装为 Deployment(含 replicas/selector/labels/ownerReferences)',
        'KubernetesTaskManagerFactory.buildTaskManagerKubernetesPod(): 装饰器链类似但不含 Service 相关装饰器，返回 KubernetesPod',
        '装饰器执行顺序: Init -> EnvSecrets -> MountSecrets -> PVC -> Cmd -> InternalService -> ExternalService -> HadoopConf -> Kerberos -> FlinkConf -> PodTemplateMount',
        '返回 KubernetesJobManagerSpecification: 封装 Deployment + 附属资源列表(Service/ConfigMap)',
    ]:
        story+=[Paragraph(f'  \u2022 {r}',st['BL'])]

    # 2.6 其他核心类
    story+=[Paragraph('2.6 其他核心类',st['H2'])]
    t2=Table([['类名','职责'],
              ['FlinkKubeClient','K8s 客户端接口: 定义 Pod/Service/ConfigMap 操作、Watch、Leader 选举等全部 K8s 交互 API'],
              ['FlinkKubeClientFactory','单例工厂: 基于 Configuration 创建 Fabric8FlinkKubeClient，配置命名空间/context/kubeconfig'],
              ['KubernetesJobManager\nSpecification','JM 规格封装: 持有 Deployment + 附属 K8s 资源列表(Service/ConfigMap)'],
              ['KubernetesWorkerNode','Worker 节点抽象: 封装 ResourceID(格式: podName)，代表一个 TM Pod'],
              ['KubernetesSessionCli','Session 集群 CLI: 解析 K8s Session 参数，创建/连接 Session 集群，使用 .yarn-properties 文件'],
              ['KubernetesEntrypointUtils','入口工具类: 加载配置，HA 模式设置 Pod IP，HostNetwork 模式端口设为 0'],
              ['KubernetesResourceManager\nFactory','RM 工厂(单例): 创建 KubernetesResourceManagerDriver，内部创建 FlinkKubeClient'],
              ['KubernetesUtils','工具类(637行): 构建启动命令、加载 Pod 模板、解析动态属性、构建 REST 端点'],
              ['Endpoint','端点封装: 地址(address) + 端口(port)，用于 REST 服务'],
              ['Constants','常量定义(138行): 容器名/卷名/挂载路径/端口名/标签键等全部 K8s 资源命名常量']],colWidths=[130,310])
    t2.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),colors.HexColor('#1565c0')),('TEXTCOLOR',(0,0),(-1,0),colors.white),
        ('FONTNAME',(0,0),(-1,-1),FONT_CN),('FONTSIZE',(0,0),(-1,-1),8),('ALIGN',(0,0),(0,-1),'LEFT'),('ALIGN',(1,0),(1,-1),'LEFT'),
        ('GRID',(0,0),(-1,-1),0.5,colors.HexColor('#b0bec5')),('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.HexColor('#fafafa'),colors.HexColor('#f5f5f5')]),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),('TOPPADDING',(0,0),(-1,-1),4),('BOTTOMPADDING',(0,0),(-1,-1),4)]))
    story+=[t2,PageBreak()]

    # ==================== 第三章 ====================
    story+=[Paragraph('三、Session 集群部署流程',st['H1']),HRFlowable(width="100%",thickness=1,color=colors.HexColor('#e0e0e0'))]
    story+=[Paragraph('Session 集群部署是 flink-kubernetes 模块最常用的部署模式。Session 集群会常驻运行一个 JM Deployment，后续通过 REST API 提交多个 Job 到同一集群。',st['B'])]
    story+=[fig_deploy_session(),Paragraph('图 3-1: Session 集群部署核心流程',st['Cap'])]
    story+=[Paragraph('关键实现细节:',st['H3'])]
    for d2 in [
        'KubernetesClusterClientFactory 通过 SPI 机制自动发现，执行目标为 "kubernetes-session"',
        'deploySessionCluster() 设置 KubernetesConfigOptionsInternal.ENTRY_POINT_CLASS 为 KubernetesSessionClusterEntrypoint',
        'deployClusterInternal() 核心逻辑: 构建 JM Parameters -> 可选加载 Pod 模板 -> KubernetesJobManagerFactory 链式构建 -> createJobManagerComponent()',
        'createJobManagerComponent() 先创建 Deployment，然后设置 OwnerReference，再创建附属资源(Service/ConfigMap)',
        'OwnerReference 机制: 所有附属资源的 owner 设为 Deployment，删除 Deployment 时 K8s 自动级联删除',
        '部署完成后轮询 REST 端点，等待 JM 启动并就绪后返回 RestClusterClient',
    ]:
        story+=[Paragraph(f'  \u2022 {d2}',st['BL'])]
    story+=[PageBreak()]

    # ==================== 第四章 ====================
    story+=[Paragraph('四、Application 集群部署流程',st['H1']),HRFlowable(width="100%",thickness=1,color=colors.HexColor('#e0e0e0'))]
    story+=[Paragraph('Application 模式是生产推荐的部署方式。用户 JAR 在 JM Pod 内的 main() 方法中执行，Job Graph 在 AM 内生成，实现了 Job 级别的完全资源隔离。',st['B'])]
    story+=[fig_deploy_application(),Paragraph('图 4-1: Application 集群部署流程',st['Cap'])]
    story+=[Paragraph('与 Session 模式的关键差异:',st['H3'])]
    for d2 in [
        'Artifact 上传: 通过 KubernetesArtifactUploader 将用户 JAR 上传到分布式存储(DFS)，或直接打入容器镜像',
        '入口点类: KubernetesApplicationClusterEntrypoint 在 AM Pod 内执行用户 main() 方法',
        'main() 执行流程: 安装 FileSystem -> 安装 SecurityContext -> 获取 PackagedProgram -> 调用 invokeInteractiveModeForExecution()',
        'JM Pod 内部构建 JobGraph，无需客户端参与，减少客户端资源消耗',
        '每个 Application 独立拥有 JM + TMs，Job 级别完全隔离',
    ]:
        story+=[Paragraph(f'  \u2022 {d2}',st['BL'])]
    story+=[PageBreak()]

    # ==================== 第五章 ====================
    story+=[Paragraph('五、TM Pod 资源管理流程',st['H1']),HRFlowable(width="100%",thickness=1,color=colors.HexColor('#e0e0e0'))]
    story+=[Paragraph('资源管理流程运行在 JM Pod 内部，由 KubernetesResourceManagerDriver 驱动。通过 Fabric8 K8s Client 创建/删除 TM Pod，并通过 Pod Watch 机制感知 Pod 生命周期事件。',st['B'])]
    story+=[fig_resource_management(),Paragraph('图 5-1: TM Pod 资源管理核心流程(时序图)',st['Cap'])]
    story+=[Paragraph('核心交互流程:',st['H3'])]
    for d2 in [
        '初始化: initializeInternal() 启动 Pod Watch、加载 TM Pod 模板、恢复之前的 Worker 节点',
        '资源请求: SlotManager 通过 requestResource() 发起，驱动创建新的 TM Pod',
        'Pod 构建: KubernetesTaskManagerFactory 通过装饰器链构建完整的 TM Pod spec',
        '异步创建: flinkKubeClient.createTaskManagerPod() 在 IO 线程池异步执行',
        'Pod Watch 回调: onAdded(完成 requestResource Future) / onModified(更新状态) / onDeleted(回收资源) / onError(错误处理)',
        'RM Failover 恢复: recoverWorkerNodesFromPreviousAttempts() 列出现有 TM Pod 并恢复为 Worker 节点',
        '资源释放: releaseResource() 调用 flinkKubeClient.stopPod() 删除 Pod',
        'OwnerReference: TM Pod 的 owner 设为 JM Deployment，Deployment 删除时自动级联删除所有 TM Pod',
    ]:
        story+=[Paragraph(f'  \u2022 {d2}',st['BL'])]
    story+=[PageBreak()]

    # ==================== 第六章 ====================
    story+=[Paragraph('六、装饰器模式与 Pod 构建',st['H1']),HRFlowable(width="100%",thickness=1,color=colors.HexColor('#e0e0e0'))]
    story+=[Paragraph('装饰器模式是 flink-kubernetes 模块最核心的设计模式。KubernetesStepDecorator 接口定义了两个方法: decorateFlinkPod() 增强 Pod 配置，buildAccompanyingKubernetesResources() 构建附属 K8s 资源。每个装饰器关注单一职责，通过链式调用组合出完整的 Pod 规格。',st['B'])]
    story+=[fig_decorator_chain(),Paragraph('图 6-1: 装饰器链式构建 JM Pod 流程',st['Cap'])]
    story+=[Paragraph('各装饰器详细职责:',st['H3'])]
    t3=Table([['装饰器','操作对象','具体职责'],
              ['InitJobManager\nDecorator','Pod + Container','设置镜像、CPU Request/Limit、Memory、端口(REST/RPC/Blob)、\n标签、注解、NodeSelector、Tolerations、ServiceAccount'],
              ['InitTaskManager\nDecorator','Pod + Container','类似 JM Init + 设置 blockedNodes 反亲和性'],
              ['CmdJobManager\nDecorator','Container','设置 JM 启动命令: kubernetes-jobmanager.sh + 参数'],
              ['CmdTaskManager\nDecorator','Container','设置 TM 启动命令 + JVM 内存参数 (-Xms/-Xmx/-XX:MaxDirectMemory/-XX:MaxMetaspace)'],
              ['ExternalService\nDecorator','附属资源','创建 REST Service (类型由 kubernetes.rest-service.exposed.type 控制)'],
              ['InternalService\nDecorator','附属资源','创建 Headless Service (非 HA 场景，用于 JM/TM 内部通信)'],
              ['FlinkConfMount\nDecorator','Pod + Container\n+ 附属资源','创建 ConfigMap(含 config.yaml + log4j/logback 配置)，\n挂载到 /opt/flink/conf'],
              ['HadoopConfMount\nDecorator','Pod + Container','挂载已有的 Hadoop 配置 ConfigMap，设置 HADOOP_CONF_DIR 环境变量'],
              ['KerberosMount\nDecorator','Pod + Container\n+ 附属资源','挂载 Kerberos keytab 和 krb5.conf，设置 KRB5_CONFIG 环境变量'],
              ['MountSecrets\nDecorator','Pod','将 K8s Secret 作为 Volume 挂载到指定路径'],
              ['EnvSecrets\nDecorator','Container','通过 secretKeyRef 将 K8s Secret 值注入为环境变量'],
              ['PodTemplateMount\nDecorator','Pod + Container\n+ 附属资源','将 TM Pod 模板内容写入 ConfigMap，挂载到 JM Pod 的 /opt/flink/pod-template.yaml'],
              ['PVCMount\nDecorator','Pod + Container','挂载 PersistentVolumeClaim，支持 readOnly 配置']],colWidths=[85,80,275])
    t3.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),colors.HexColor('#2e7d32')),('TEXTCOLOR',(0,0),(-1,0),colors.white),
        ('FONTNAME',(0,0),(-1,-1),FONT_CN),('FONTSIZE',(0,0),(-1,-1),7),('ALIGN',(0,0),(-1,-1),'LEFT'),
        ('GRID',(0,0),(-1,-1),0.5,colors.HexColor('#b0bec5')),('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.HexColor('#fafafa'),colors.HexColor('#f5f5f5')]),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),('TOPPADDING',(0,0),(-1,-1),3),('BOTTOMPADDING',(0,0),(-1,-1),3)]))
    story+=[t3,PageBreak()]

    # ==================== 第七章 ====================
    story+=[Paragraph('七、Session vs Application 模式对比',st['H1']),HRFlowable(width="100%",thickness=1,color=colors.HexColor('#e0e0e0'))]
    story+=[Paragraph('Flink on Kubernetes 支持两种部署模式，通过 KubernetesDeploymentTarget 枚举区分: kubernetes-session 和 kubernetes-application。两种模式在入口点、Job 图生成位置和资源隔离性上有本质差异。',st['B'])]
    story+=[fig_modes_comparison(),Paragraph('图 7-1: Session 模式 vs Application 模式对比',st['Cap'])]
    story+=[Paragraph('Session 模式详解:',st['H3'])]
    for d2 in [
        '入口点: KubernetesSessionClusterEntrypoint -> SessionClusterEntrypoint',
        '部署: deploySessionCluster() -> deployClusterInternal()，JM 常驻等待 Job 提交',
        'Job 图由客户端生成并通过 REST API 提交到已运行的 Session 集群',
        '多个 Job 共享同一个 JM 和 TM 资源池，适合交互式开发和短任务',
        'TM 资源按需分配，空闲超时后自动回收',
    ]:
        story+=[Paragraph(f'  \u2022 {d2}',st['BL'])]
    story+=[Paragraph('Application 模式详解:',st['H3'])]
    for d2 in [
        '入口点: KubernetesApplicationClusterEntrypoint -> ApplicationClusterEntryPoint',
        '部署: deployApplicationCluster()，用户 JAR 被打入镜像或通过 Artifact 上传',
        'JM Pod 内通过 DefaultPackagedProgramRetriever 获取 PackagedProgram 并执行 main()',
        '每个 Application 独立拥有 JM 和 TM，Job 级别完全隔离',
        '生产推荐模式: 更好的安全隔离和资源管理',
    ]:
        story+=[Paragraph(f'  \u2022 {d2}',st['BL'])]
    story+=[PageBreak()]

    # ==================== 第八章 ====================
    story+=[Paragraph('八、K8s Service 暴露策略',st['H1']),HRFlowable(width="100%",thickness=1,color=colors.HexColor('#e0e0e0'))]
    story+=[Paragraph('Flink 通过 ServiceType 策略模式支持四种 K8s Service 类型，控制 REST API 端点的暴露方式。每种 Service 类型实现了构建外部/内部 Service 和获取端点的方法。',st['B'])]
    story+=[fig_service_types(),Paragraph('图 8-1: Kubernetes Service 暴露策略',st['Cap'])]
    story+=[Paragraph('各 Service 类型说明:',st['H3'])]
    t4=Table([['Service 类型','K8s type','端点获取方式','适用场景'],
              ['ClusterIP','ClusterIP','clusterIP:port','集群内部访问(默认)'],
              ['Headless ClusterIP','ClusterIP\n(clusterIP=None)','Pod IP 直连','HA 模式 / 直连 Pod'],
              ['NodePort','NodePort','nodeIP:nodePort','开发测试环境'],
              ['LoadBalancer','LoadBalancer','外部 IP:port\n(等待分配)','生产环境 / 云厂商']],colWidths=[90,80,110,140])
    t4.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),colors.HexColor('#1565c0')),('TEXTCOLOR',(0,0),(-1,0),colors.white),
        ('FONTNAME',(0,0),(-1,-1),FONT_CN),('FONTSIZE',(0,0),(-1,-1),8),('ALIGN',(0,0),(-1,-1),'CENTER'),
        ('GRID',(0,0),(-1,-1),0.5,colors.HexColor('#b0bec5')),('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.HexColor('#fafafa'),colors.HexColor('#f5f5f5')]),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),('TOPPADDING',(0,0),(-1,-1),4),('BOTTOMPADDING',(0,0),(-1,-1),4)]))
    story+=[t4,Spacer(1,10)]
    story+=[Paragraph('NodePort 地址类型: 通过 kubernetes.rest-service.exposed.node-port-address-type 配置，支持 InternalIP 和 ExternalIP 两种模式。',st['B'])]
    story+=[PageBreak()]

    # ==================== 第九章 ====================
    story+=[Paragraph('九、高可用 (HA) 机制',st['H1']),HRFlowable(width="100%",thickness=1,color=colors.HexColor('#e0e0e0'))]
    story+=[Paragraph('flink-kubernetes 模块实现了基于 K8s ConfigMap 的原生 HA 方案，替代传统的 ZooKeeper 方案。Leader 选举、状态存储和服务发现全部通过 ConfigMap 实现。',st['B'])]
    story+=[fig_ha_leader_election(),Paragraph('图 9-1: 基于 ConfigMap 的 HA Leader 选举流程',st['Cap'])]

    story+=[Paragraph('核心 HA 组件:',st['H3'])]
    t5=Table([['组件','职责'],
              ['KubernetesLeaderElection\nHaServices','HA 服务实现: 继承 AbstractHaServices，创建基于 ConfigMap 的 Leader 选举和状态存储实例'],
              ['KubernetesLeaderElection\nDriver','Leader 选举驱动: 通过 K8s LeaderElector API 实现竞选，监听 isLeader/notLeader 回调'],
              ['KubernetesLeaderRetrieval\nDriver','Leader 发现驱动: 通过 ConfigMapSharedWatcher 监听 ConfigMap 变化，获取当前 Leader 地址'],
              ['KubernetesStateHandleStore','状态存储: 基于 ConfigMap 的分布式状态存储，支持乐观锁(CAS)更新'],
              ['KubernetesConfigMap\nSharedWatcher','共享 Watcher: 单个 K8s Watch 连接监控多个 ConfigMap，减少 API Server 负载']],colWidths=[130,310])
    t5.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),colors.HexColor('#4a148c')),('TEXTCOLOR',(0,0),(-1,0),colors.white),
        ('FONTNAME',(0,0),(-1,-1),FONT_CN),('FONTSIZE',(0,0),(-1,-1),8),('ALIGN',(0,0),(-1,-1),'LEFT'),
        ('GRID',(0,0),(-1,-1),0.5,colors.HexColor('#b0bec5')),('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.HexColor('#fafafa'),colors.HexColor('#f5f5f5')]),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),('TOPPADDING',(0,0),(-1,-1),4),('BOTTOMPADDING',(0,0),(-1,-1),4)]))
    story+=[t5,Spacer(1,10)]

    story+=[Paragraph('ConfigMap 命名规则:',st['H3'])]
    for d2 in [
        '集群 ConfigMap: {clusterId}-cluster-config-map (存储 Leader 信息)',
        'Job ConfigMap: {clusterId}-{jobId}-config-map (存储 Job 级别的 Checkpoint 状态)',
        'Leader 信息键: componentId.leader (存储 leader 的 URL 和 UUID)',
        '状态句柄键: 使用 key-value 方式存储序列化的 StateHandle',
    ]:
        story+=[Paragraph(f'  \u2022 {d2}',st['BL'])]

    story+=[Paragraph('乐观锁更新机制:',st['H3'])]
    story+=[Paragraph('KubernetesStateHandleStore 和 LeaderElectionDriver 使用 checkAndUpdateConfigMap() 方法实现乐观锁更新。该方法先读取 ConfigMap 的 resourceVersion，更新后带 resourceVersion 提交，如果版本冲突(被其他节点修改)则重试。最大重试次数由 kubernetes.transactional-operation.max-retries 配置(默认5次)。',st['B'])]
    story+=[PageBreak()]

    # ==================== 第十章 ====================
    story+=[Paragraph('十、参数与配置体系',st['H1']),HRFlowable(width="100%",thickness=1,color=colors.HexColor('#e0e0e0'))]

    story+=[Paragraph('10.1 参数层次结构',st['H2'])]
    story+=[Paragraph('flink-kubernetes 使用三层参数体系，从通用到特定逐层扩展:',st['B'])]
    t6=Table([['层次','类名','核心参数'],
              ['接口层','KubernetesParameters','定义 40+ 个参数获取方法 (clusterId/namespace/image/labels/tolerations/secrets/PVC 等)'],
              ['抽象基类','AbstractKubernetes\nParameters','持有 Configuration，实现通用参数获取逻辑，231 行'],
              ['JM 特化','KubernetesJobManager\nParameters','CPU/Memory/LimitFactor、restPort、rpcPort、blobServerPort、\nserviceAccount、entrypointClass、restServiceExposedType、replicas、podTemplate、ownerReference'],
              ['TM 特化','KubernetesTaskManager\nParameters','podName、dynamicProperties、jvmMemOpts、containeredTMParams、\nexternalResourceConfigKeys、blockedNodes']],colWidths=[60,110,270])
    t6.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),colors.HexColor('#1565c0')),('TEXTCOLOR',(0,0),(-1,0),colors.white),
        ('FONTNAME',(0,0),(-1,-1),FONT_CN),('FONTSIZE',(0,0),(-1,-1),8),('ALIGN',(0,0),(-1,-1),'LEFT'),
        ('GRID',(0,0),(-1,-1),0.5,colors.HexColor('#b0bec5')),('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.HexColor('#fafafa'),colors.HexColor('#f5f5f5')]),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),('TOPPADDING',(0,0),(-1,-1),4),('BOTTOMPADDING',(0,0),(-1,-1),4)]))
    story+=[t6,Spacer(1,10)]

    story+=[Paragraph('10.2 核心配置项 (KubernetesConfigOptions)',st['H2'])]
    story+=[Paragraph('KubernetesConfigOptions 是最大的配置定义文件(777行，42.9KB)，包含 40+ 个配置项:',st['B'])]
    t7=Table([['配置分类','关键配置项','说明'],
              ['集群','cluster-id, namespace, context,\nkube-config-file','集群标识、命名空间、kubeconfig'],
              ['镜像','container-image, image-pull-policy,\nimage-pull-secrets','容器镜像、拉取策略、密钥'],
              ['JM 资源','jobmanager.cpu, jobmanager.cpu-limit-factor,\njobmanager.memory-limit-factor','JM CPU/内存 Request 和 Limit 比例'],
              ['TM 资源','taskmanager.cpu, taskmanager.cpu-limit-factor,\ntaskmanager.memory-limit-factor','TM CPU/内存 Request 和 Limit 比例'],
              ['Service','rest-service.exposed.type','ClusterIP/NodePort/LoadBalancer/Headless'],
              ['标签/注解','jobmanager-labels, taskmanager-labels,\njobmanager-annotations, rest-service-annotations','Pod 和 Service 的标签/注解'],
              ['Secrets','kubernetes.secrets, kubernetes.env.\nsecret-key-ref','Secret 卷挂载和环境变量注入'],
              ['Pod 模板','pod-template-file.jobmanager,\npod-template-file.taskmanager','Pod 模板 YAML 文件路径'],
              ['PVC','persistent-volume-claims,\npersistent-volume-claim-read-only','PVC 名称和只读配置'],
              ['网络','hostnetwork-enabled, rest-service.\nexposed.node-port-address-type','HostNetwork/NodePort 地址类型'],
              ['HA','jobmanager.replicas','JM 副本数(HA)'],
              ['Hadoop','hadoop-conf-config-map,\nhadoop-conf-mount-decorator-enabled','Hadoop 配置 ConfigMap 和启用开关'],
              ['Artifact','local-upload-enabled, local-upload-target,\nlocal-upload-overwrite','Artifact 上传开关/目标/覆盖策略'],
              ['重试','transactional-operation.max-retries','乐观锁重试次数(默认5)'],
              ['IO 线程','client-io-executor-pool-size','K8s 客户端 IO 线程池大小(默认4)']],colWidths=[60,170,210])
    t7.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),colors.HexColor('#1565c0')),('TEXTCOLOR',(0,0),(-1,0),colors.white),
        ('FONTNAME',(0,0),(-1,-1),FONT_CN),('FONTSIZE',(0,0),(-1,-1),7),('ALIGN',(0,0),(-1,-1),'LEFT'),
        ('GRID',(0,0),(-1,-1),0.5,colors.HexColor('#b0bec5')),('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.HexColor('#fafafa'),colors.HexColor('#f5f5f5')]),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),('TOPPADDING',(0,0),(-1,-1),3),('BOTTOMPADDING',(0,0),(-1,-1),3)]))
    story+=[t7,Spacer(1,10)]

    story+=[Paragraph('10.3 内部枚举类型',st['H2'])]
    for d2 in [
        'ServiceExposedType: ClusterIP / NodePort / LoadBalancer / Headless_ClusterIP',
        'NodePortAddressType: InternalIP / ExternalIP',
        'ImagePullPolicy: IfNotPresent / Always / Never',
        'KubernetesDeploymentTarget: SESSION("kubernetes-session") / APPLICATION("kubernetes-application")',
    ]:
        story+=[Paragraph(f'  \u2022 {d2}',st['BL'])]
    story+=[PageBreak()]

    # ==================== 第十一章 ====================
    story+=[Paragraph('十一、Pod 模板支持',st['H1']),HRFlowable(width="100%",thickness=1,color=colors.HexColor('#e0e0e0'))]
    story+=[Paragraph('flink-kubernetes 支持用户自定义 Pod 模板(YAML 格式)，可对 JM 和 TM Pod 进行灵活定制。Pod 模板中的配置会被装饰器的显式配置覆盖/合并。',st['B'])]

    story+=[Paragraph('11.1 Pod 模板加载流程',st['H2'])]
    for d2 in [
        'JM Pod 模板: 通过 kubernetes.pod-template-file.jobmanager 配置文件路径',
        'TM Pod 模板: 通过 kubernetes.pod-template-file.taskmanager 配置文件路径',
        'JM 端加载: KubernetesClusterDescriptor.deployClusterInternal() 中通过 loadPodFromTemplateFile() 加载，传给工厂',
        'TM 端传递: PodTemplateMountDecorator 将 TM Pod 模板内容写入 ConfigMap，挂载到 JM Pod',
        'TM 端加载: KubernetesResourceManagerDriver.initializeInternal() 从挂载文件加载 TM Pod 模板',
    ]:
        story+=[Paragraph(f'  \u2022 {d2}',st['BL'])]

    story+=[Paragraph('11.2 模板合并规则',st['H2'])]
    for d2 in [
        '模板中主容器名必须为 "flink-main-container"，否则使用空容器初始化',
        'FlinkPod 将 Pod 拆分为 podWithoutMainContainer + mainContainer，分别处理',
        '装饰器的显式配置优先于模板配置(装饰器会覆盖或追加)',
        '模板中的自定义 sidecar 容器、init containers 等会被保留',
        '资源限制(CPU/Memory)由装饰器设置，模板中的设置会被覆盖',
    ]:
        story+=[Paragraph(f'  \u2022 {d2}',st['BL'])]

    story+=[Paragraph('11.3 Pod 模板示例',st['H2'])]
    story+=[Paragraph('一个典型的 TM Pod 模板 YAML:',st['B'])]
    for line in [
        'apiVersion: v1',
        'kind: Pod',
        'metadata:',
        '  annotations:',
        '    custom-annotation: custom-value',
        'spec:',
        '  containers:',
        '    - name: flink-main-container  # 必须使用此名称',
        '      volumeMounts:',
        '        - name: custom-volume',
        '          mountPath: /opt/custom',
        '    - name: sidecar  # 自定义 sidecar 容器',
        '      image: my-sidecar:latest',
        '  volumes:',
        '    - name: custom-volume',
        '      emptyDir: {}',
    ]:
        story+=[Paragraph(line,st['CodeBlock'])]
    story+=[PageBreak()]

    # ==================== 第十二章 ====================
    story+=[Paragraph('十二、关键设计模式与架构思想',st['H1']),HRFlowable(width="100%",thickness=1,color=colors.HexColor('#e0e0e0'))]
    patterns=[
        ('装饰器模式 (Decorator)',
         'KubernetesStepDecorator 链式增强 FlinkPod，每个装饰器关注单一职责(镜像设置、命令注入、配置挂载、Service 创建等)。这是模块最核心的设计模式，实现了 Pod 配置的高度可扩展性和可组合性。'),
        ('工厂模式 (Factory)',
         'FlinkKubeClientFactory(单例)、KubernetesClusterClientFactory(SPI 注册)、KubernetesResourceManagerFactory(单例)、KubernetesJobManagerFactory/KubernetesTaskManagerFactory(静态工厂) — 所有核心组件通过工厂创建，解耦实例化逻辑。'),
        ('策略模式 (Strategy)',
         'ServiceType 抽象不同的 K8s Service 暴露策略(ClusterIP/HeadlessClusterIP/NodePort/LoadBalancer)，运行时根据配置选择具体策略。'),
        ('建造者模式 (Builder)',
         'FlinkPod.Builder 链式构建 Pod 对象；Fabric8 的 PodBuilder/ContainerBuilder/DeploymentBuilder 用于构建 K8s 资源对象。'),
        ('观察者模式 (Observer)',
         'FlinkKubeClient.WatchCallbackHandler&lt;T&gt; 处理 Pod/ConfigMap Watch 事件(onAdded/onModified/onDeleted/onError)，实现 K8s 资源的实时监控。'),
        ('模板方法模式 (Template Method)',
         'AbstractResourceManagerDriver.initializeInternal() 由子类 KubernetesResourceManagerDriver 实现；AbstractKubernetesParameters 定义参数获取骨架，子类实现特定参数。'),
        ('单例模式 (Singleton)',
         'FlinkKubeClientFactory.INSTANCE / KubernetesResourceManagerFactory.INSTANCE — 确保全局唯一的客户端工厂和资源管理工厂。'),
        ('SPI 服务发现',
         'KubernetesSessionClusterExecutorFactory 通过 META-INF/services 注册，实现插件化部署目标。KubernetesClusterClientFactory 同样通过 SPI 机制自动发现。'),
        ('CompletableFuture 异步编排',
         'requestResource() 返回 CompletableFuture&lt;KubernetesWorkerNode&gt;，Pod 创建后 complete()。Fabric8FlinkKubeClient 内部使用 ScheduledExecutorService 实现异步 IO。'),
        ('OwnerReference 级联删除',
         '所有附属资源(Service/ConfigMap/TM Pod)的 OwnerReference 设为 JM Deployment，删除 Deployment 时 K8s 自动级联删除全部关联资源，避免资源泄漏。'),
    ]
    for name,desc in patterns:
        story+=[Paragraph(f'<b>{name}</b>',st['H3'])]
        story+=[Paragraph(desc,st['BI'])]

    story+=[Spacer(1,30),HRFlowable(width="100%",thickness=2,color=colors.HexColor('#1565c0'))]
    story+=[Paragraph('文档结束 — 基于 Flink 1.15.4 flink-kubernetes 模块全部源码分析生成',st['Cap'])]

    doc.build(story)
    print(f"PDF generated: {out}")
    return out

if __name__ == '__main__':
    build()
