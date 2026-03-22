#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Flink Runtime 核心流程与设计说明 PDF 生成器"""
import os, math
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.lib.colors import HexColor, black, white
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, PageBreak, Table, TableStyle, Flowable)
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

# === 颜色 ===
CB = HexColor('#1A237E'); C1 = HexColor('#1565C0'); C2 = HexColor('#0277BD')
C3 = HexColor('#00838F'); CG = HexColor('#2E7D32'); CO = HexColor('#E65100')
CP = HexColor('#6A1B9A'); CR = HexColor('#C62828'); CGY = HexColor('#757575')
CLB = HexColor('#E3F2FD'); CLG = HexColor('#E8F5E9'); CLO = HexColor('#FFF3E0')
CLP = HexColor('#F3E5F5'); CLY = HexColor('#FFF8E1'); CLGY = HexColor('#F5F5F5')
CBD = HexColor('#BDBDBD')

def register_fonts():
    paths = ['/System/Library/Fonts/PingFang.ttc','/System/Library/Fonts/STHeiti Light.ttc',
             '/System/Library/Fonts/Hiragino Sans GB.ttc','/Library/Fonts/Arial Unicode.ttf',
             '/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc','/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc']
    for p in paths:
        if os.path.exists(p):
            try:
                pdfmetrics.registerFont(TTFont('CF', p, subfontIndex=0))
                try: pdfmetrics.registerFont(TTFont('CFB', p, subfontIndex=1))
                except: pdfmetrics.registerFont(TTFont('CFB', p, subfontIndex=0))
                return True
            except: continue
    pdfmetrics.registerAlias('CF','Helvetica'); pdfmetrics.registerAlias('CFB','Helvetica-Bold')
    return False

class FlowChart(Flowable):
    def __init__(s, w, h, fn): Flowable.__init__(s); s.width=w; s.height=h; s.fn=fn
    def draw(s): s.fn(s.canv, s.width, s.height)

def rrect(c,x,y,w,h,r,fc,text="",fs=8):
    c.saveState(); c.setFillColor(fc); c.setStrokeColor(fc); c.setLineWidth(1)
    c.roundRect(x,y,w,h,r,stroke=1,fill=1)
    if text:
        is_light = fc in (CLB,CLG,CLO,CLP,CLY,white,CLGY,HexColor('#FFFFFF'),HexColor('#F8F9FA'))
        c.setFillColor(black if is_light else white); c.setFont('CF',fs)
        lines=text.split('\n'); th=len(lines)*(fs+2); sy=y+h/2+th/2-fs
        for i,l in enumerate(lines):
            tw=c.stringWidth(l,'CF',fs); c.drawString(x+(w-tw)/2, sy-i*(fs+2), l)
    c.restoreState()

def arrow(c,x1,y1,x2,y2,clr=black):
    c.saveState(); c.setStrokeColor(clr); c.setFillColor(clr); c.setLineWidth(1.2)
    c.line(x1,y1,x2,y2); a=math.atan2(y2-y1,x2-x1); al=6; aa=math.pi/6
    p=c.beginPath(); p.moveTo(x2,y2)
    p.lineTo(x2-al*math.cos(a-aa),y2-al*math.sin(a-aa))
    p.lineTo(x2-al*math.cos(a+aa),y2-al*math.sin(a+aa)); p.close()
    c.drawPath(p,fill=1); c.restoreState()

def darrow(c,x1,y1,x2,y2,clr=CGY):
    c.saveState(); c.setStrokeColor(clr); c.setFillColor(clr); c.setLineWidth(1); c.setDash(3,3)
    c.line(x1,y1,x2,y2); c.setDash(); a=math.atan2(y2-y1,x2-x1); al=5; aa=math.pi/6
    p=c.beginPath(); p.moveTo(x2,y2)
    p.lineTo(x2-al*math.cos(a-aa),y2-al*math.sin(a-aa))
    p.lineTo(x2-al*math.cos(a+aa),y2-al*math.sin(a+aa)); p.close()
    c.drawPath(p,fill=1); c.restoreState()

def mkstyles():
    s=getSampleStyleSheet()
    def a(n,**kw): s.add(ParagraphStyle(n,**kw))
    a('CT',fontName='CFB',fontSize=32,leading=44,alignment=TA_CENTER,textColor=CB,spaceAfter=20)
    a('CS',fontName='CF',fontSize=16,leading=24,alignment=TA_CENTER,textColor=CGY,spaceAfter=10)
    a('H0',fontName='CFB',fontSize=22,leading=30,textColor=CB,spaceBefore=20,spaceAfter=16)
    a('H1',fontName='CFB',fontSize=16,leading=22,textColor=C1,spaceBefore=16,spaceAfter=10)
    a('H2',fontName='CFB',fontSize=13,leading=18,textColor=C2,spaceBefore=12,spaceAfter=8)
    a('H3',fontName='CFB',fontSize=11,leading=15,textColor=C3,spaceBefore=8,spaceAfter=6)
    a('BD',fontName='CF',fontSize=10,leading=16,textColor=black,spaceBefore=3,spaceAfter=3,alignment=TA_JUSTIFY)
    a('BL',fontName='CF',fontSize=10,leading=15,textColor=black,spaceBefore=2,spaceAfter=2,leftIndent=20,bulletIndent=8)
    a('TE',fontName='CF',fontSize=12,leading=20,textColor=C1,spaceBefore=4,spaceAfter=4)
    a('TS',fontName='CF',fontSize=10,leading=16,textColor=HexColor('#555555'),spaceBefore=2,spaceAfter=2,leftIndent=20)
    return s

def mktable(hdrs,rows,cw=None):
    if cw is None: cw=[480//len(hdrs)]*len(hdrs)
    d=[hdrs]+rows; t=Table(d,colWidths=cw,repeatRows=1)
    t.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),C1),('TEXTCOLOR',(0,0),(-1,0),white),
        ('FONTNAME',(0,0),(-1,0),'CFB'),('FONTSIZE',(0,0),(-1,0),9),
        ('FONTNAME',(0,1),(-1,-1),'CF'),('FONTSIZE',(0,1),(-1,-1),8),
        ('ALIGN',(0,0),(-1,-1),'LEFT'),('VALIGN',(0,0),(-1,-1),'MIDDLE'),
        ('GRID',(0,0),(-1,-1),0.5,CBD),('ROWBACKGROUNDS',(0,1),(-1,-1),[white,CLGY]),
        ('TOPPADDING',(0,0),(-1,-1),4),('BOTTOMPADDING',(0,0),(-1,-1),4),
        ('LEFTPADDING',(0,0),(-1,-1),6),('RIGHTPADDING',(0,0),(-1,-1),6)]))
    return t

def infobox(text,bg=CLB,bc=C1):
    p=Paragraph(text,ParagraphStyle('ib',fontName='CF',fontSize=9,leading=14,textColor=HexColor('#333333')))
    t=Table([[p]],colWidths=[480])
    t.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,-1),bg),('LEFTPADDING',(0,0),(-1,-1),12),
        ('RIGHTPADDING',(0,0),(-1,-1),12),('TOPPADDING',(0,0),(-1,-1),8),('BOTTOMPADDING',(0,0),(-1,-1),8),
        ('LINEBEFORESTYLE',(0,0),(0,-1),'SOLID'),('LINEBEFORECOLOR',(0,0),(0,-1),bc),('LINEBEFOREWIDTH',(0,0),(0,-1),3)]))
    return t

def hf(cv,doc):
    cv.saveState(); w,h=A4
    cv.setStrokeColor(C1); cv.setLineWidth(1.5); cv.line(30,h-40,w-30,h-40)
    cv.setFont('CF',8); cv.setFillColor(CGY)
    cv.drawString(30,h-36,"Flink Runtime 核心源码分析"); cv.drawRightString(w-30,h-36,"基于 Flink 1.15.4")
    cv.setStrokeColor(CBD); cv.setLineWidth(0.5); cv.line(30,35,w-30,35)
    cv.drawCentredString(w/2,22,f"第 {doc.page} 页"); cv.restoreState()

# ========== 流程图绘制 ==========
def draw_startup(c,w,h):
    c.setFont('CFB',11); c.setFillColor(CB); c.drawCentredString(w/2,h-15,"Flink 集群启动完整流程")
    bw,bh=130,28; y=h-45
    rrect(c,w/2-bw/2,y,bw,bh,5,C1,text="ClusterEntrypoint",fs=9); arrow(c,w/2,y,w/2,y-20)
    y-=20+bh; rrect(c,w/2-bw/2,y,bw,bh,5,C2,text="startCluster()",fs=9); arrow(c,w/2,y,w/2,y-20)
    y-=20+bh; rrect(c,w/2-bw/2-20,y,bw+40,bh,5,C3,text="initializeServices()",fs=9)
    for i,s in enumerate(["RpcService","HA Services","BlobServer"]):
        sy=y-15-i*22; rrect(c,30,sy,90,18,3,CLB,text=s,fs=7); darrow(c,120,sy+9,w/2-bw/2-20,y+bh/2)
    for i,s in enumerate(["HeartbeatSvc","MetricRegistry","IOExecutor"]):
        sx=w-120; sy=y-15-i*22; rrect(c,sx,sy,90,18,3,CLB,text=s,fs=7); darrow(c,sx,sy+9,w/2+bw/2+20,y+bh/2)
    y4=y-85; arrow(c,w/2,y,w/2,y4+bh); rrect(c,w/2-bw/2-20,y4,bw+40,bh,5,CG,text="factory.create()",fs=9)
    y5=y4-50; cw2=120
    rrect(c,30,y5,cw2,32,5,CP,text="Dispatcher\nRunner",fs=8)
    rrect(c,w/2-cw2/2,y5,cw2,32,5,CO,text="ResourceManager\nService",fs=8)
    rrect(c,w-30-cw2,y5,cw2,32,5,CR,text="WebMonitor\nEndpoint",fs=8)
    arrow(c,w/2-40,y4,30+cw2/2,y5+32); arrow(c,w/2,y4,w/2,y5+32); arrow(c,w/2+40,y4,w-30-cw2/2,y5+32)

def draw_submit(c,w,h):
    c.setFont('CFB',11); c.setFillColor(CB); c.drawCentredString(w/2,h-15,"作业提交与执行流程")
    bh=26; y=h-45
    rrect(c,20,y,80,bh,5,CGY,text="Client",fs=9); arrow(c,100,y+bh/2,140,y+bh/2)
    rrect(c,140,y,80,bh,5,C3,text="REST API",fs=9); arrow(c,220,y+bh/2,260,y+bh/2)
    rrect(c,260,y,100,bh,5,C1,text="Dispatcher",fs=9)
    y2=y-40; arrow(c,310,y,310,y2+bh); rrect(c,260,y2,100,bh,5,C2,text="submitJob()",fs=9)
    rrect(c,420,y2,90,bh,5,CLY,text="HA Store",fs=8); arrow(c,360,y2+bh/2,420,y2+bh/2)
    y3=y2-40; arrow(c,310,y2,310,y3+bh); rrect(c,230,y3,160,bh,5,CG,text="createJobMasterRunner()",fs=8)
    y4=y3-40; arrow(c,310,y3,310,y4+bh); rrect(c,260,y4,100,bh,5,CP,text="JobMaster",fs=9)
    y5=y4-40; arrow(c,310,y4,310,y5+bh); rrect(c,245,y5,130,bh,5,CO,text="DefaultScheduler",fs=9)
    y6=y5-40
    rrect(c,40,y6,120,bh,5,CLG,text="ExecutionGraph",fs=8); arrow(c,245,y5+bh/2,160,y6+bh/2)
    rrect(c,200,y6,100,bh,5,CLO,text="SlotPool",fs=8); arrow(c,310,y5,250,y6+bh)
    rrect(c,350,y6,130,bh,5,CLP,text="Deploy Tasks",fs=8); arrow(c,375,y5,415,y6+bh)
    y7=y6-40; rrect(c,350,y7,130,bh,5,CR,text="TaskExecutor",fs=9); arrow(c,415,y6,415,y7+bh)

def draw_checkpoint(c,w,h):
    c.setFont('CFB',11); c.setFillColor(CB); c.drawCentredString(w/2,h-15,"Checkpoint 检查点完整流程")
    bh=24; y=h-45
    def lbl(txt,yy): c.setFont('CFB',9); c.setFillColor(CG); c.drawString(15,yy+8,txt)
    lbl("1.触发",y); rrect(c,60,y,130,bh,4,C1,text="CheckpointCoordinator",fs=8)
    arrow(c,190,y+bh/2,230,y+bh/2); rrect(c,230,y,120,bh,4,C2,text="triggerCheckpoint()",fs=8)
    arrow(c,350,y+bh/2,390,y+bh/2); rrect(c,390,y,100,bh,4,CG,text="PendingCP",fs=8)
    y2=y-42; lbl("2.广播",y2); rrect(c,60,y2,130,bh,4,C3,text="triggerTasks()",fs=8)
    for i in range(3):
        sx=240+i*85; rrect(c,sx,y2,75,bh,4,CLB,text=f"Source {i+1}",fs=7); arrow(c,190,y2+bh/2,sx,y2+bh/2)
    y3=y2-42; lbl("3.传播",y3)
    ops=["Source","Map/Filter","Aggregate","Sink"]; cls=[C1,C2,C3,CG]
    for i,(o,cl) in enumerate(zip(ops,cls)):
        ox=60+i*120; rrect(c,ox,y3,95,bh,4,cl,text=o,fs=8)
        if i<3: arrow(c,ox+95,y3+bh/2,ox+120,y3+bh/2); c.setFont('Courier',6); c.setFillColor(CR); c.drawCentredString(ox+107,y3+bh/2+5,"B")
    y4=y3-52; lbl("4.对齐",y4); rrect(c,60,y4,140,36,4,CLY,text="BarrierHandler\n(Exactly-Once)",fs=7)
    rrect(c,240,y4+12,100,20,3,CLB,text="Ch-0 [Barrier]",fs=6); rrect(c,240,y4-8,100,20,3,white,text="Ch-1 ...",fs=6)
    arrow(c,200,y4+18,240,y4+22); arrow(c,200,y4+18,240,y4+2)
    rrect(c,370,y4+4,100,28,4,CG,text="Snapshot!",fs=8); arrow(c,340,y4+18,370,y4+18)
    y5=y4-42; lbl("5.ACK",y5); rrect(c,60,y5,100,bh,4,CO,text="Task ACK",fs=8)
    arrow(c,160,y5+bh/2,200,y5+bh/2); rrect(c,200,y5,130,bh,4,C1,text="receiveAckMsg()",fs=7)
    arrow(c,330,y5+bh/2,370,y5+bh/2); rrect(c,370,y5,120,bh,4,CG,text="CompletedCP!",fs=8)
    y6=y5-42; lbl("6.存储",y6); rrect(c,60,y6,140,bh,4,CP,text="CompletedCPStore",fs=8)
    arrow(c,200,y6+bh/2,240,y6+bh/2); rrect(c,240,y6,100,bh,4,CLG,text="HDFS/S3",fs=8)
    arrow(c,340,y6+bh/2,380,y6+bh/2); rrect(c,380,y6,110,bh,4,CLY,text="Metadata(ZK)",fs=7)

def draw_resource(c,w,h):
    c.setFont('CFB',11); c.setFillColor(CB); c.drawCentredString(w/2,h-15,"声明式资源管理流程")
    y=h-55
    rrect(c,20,y,110,40,5,C1,text="JobMaster\nSlotPool",fs=8)
    rrect(c,190,y,120,40,5,CG,text="ResourceManager\nSlotManager",fs=8)
    rrect(c,370,y,110,40,5,CO,text="TaskExecutor",fs=8)
    arrow(c,130,y+30,190,y+30); arrow(c,310,y+30,370,y+30); arrow(c,370,y+10,310,y+10); arrow(c,190,y+10,130,y+10)
    c.setFont('CF',6); c.setFillColor(CGY)
    c.drawCentredString(160,y+34,"declareRequired"); c.drawCentredString(340,y+34,"requestSlot")
    c.drawCentredString(340,y+5,"SlotReport"); c.drawCentredString(160,y+5,"offerSlots")
    y2=y-55
    for i,(t,cl) in enumerate([("1.SlotPool\n计算需求",C1),("2.RM匹配\n可用Slot",CG),("3.TM分配\nSlot",CO),("4.TM offer\nSlot给JM",CP)]):
        sx=20+i*125; rrect(c,sx,y2,115,36,4,cl,text=t,fs=7)
        if i<3: arrow(c,sx+115,y2+18,sx+125,y2+18)
    y3=y2-55; c.setFont('CFB',9); c.setFillColor(CR); c.drawString(20,y3+32,"Active模式(YARN/K8s):")
    rrect(c,20,y3,110,28,4,CR,text="SlotManager",fs=8); arrow(c,130,y3+14,170,y3+14)
    rrect(c,170,y3,120,28,4,CO,text="ResourceAllocator",fs=8); arrow(c,290,y3+14,330,y3+14)
    rrect(c,330,y3,150,28,4,CP,text="Driver.requestResource()",fs=7)

def draw_failover(c,w,h):
    c.setFont('CFB',11); c.setFillColor(CB); c.drawCentredString(w/2,h-15,"故障恢复流程 (RestartPipelinedRegion)")
    bh=24; y=h-45
    rrect(c,20,y,90,bh,4,CR,text="Task FAILED",fs=8); arrow(c,110,y+bh/2,150,y+bh/2)
    rrect(c,150,y,110,bh,4,C1,text="onTaskFailed()",fs=8); arrow(c,260,y+bh/2,300,y+bh/2)
    rrect(c,300,y,120,bh,4,CO,text="FailureHandler",fs=8)
    y2=y-45
    rrect(c,60,y2,140,bh,4,C3,text="FailoverStrategy",fs=8); arrow(c,350,y,130,y2+bh)
    rrect(c,280,y2,150,bh,4,CP,text="RestartBackoffStrategy",fs=7); arrow(c,420,y,355,y2+bh)
    y3=y2-50; c.setFont('CFB',8); c.setFillColor(C3); c.drawString(20,y3+32,"Region分析(BFS):")
    for i,(r,cl) in enumerate([("Failed Region",CR),("Input Region",CO),("Consumer Region",CP)]):
        rx=20+i*160; rrect(c,rx,y3,140,24,4,cl,text=r,fs=8)
        if i<2: arrow(c,rx+140,y3+12,rx+160,y3+12)
    y4=y3-50; c.setFont('CFB',8); c.setFillColor(CG); c.drawString(20,y4+32,"恢复执行:")
    for i,(t,cl) in enumerate([("Cancel\nTasks",CR),("Reset\nExecution",CO),("Restore\nState",CG),("Restart\nTasks",C1)]):
        rx=20+i*125; rrect(c,rx,y4,100,28,4,cl,text=t,fs=7)
        if i<3: arrow(c,rx+100,y4+14,rx+125,y4+14)

def draw_task_lifecycle(c,w,h):
    c.setFont('CFB',11); c.setFillColor(CB); c.drawCentredString(w/2,h-15,"Task 生命周期与状态转换")
    bw,bh=90,22; cx=w/2-bw/2; y=h-45
    for st,cl in [("CREATED",CGY),("DEPLOYING",C2),("INITIALIZING",C3),("RUNNING",CG)]:
        rrect(c,cx,y,bw,bh,4,cl,text=st,fs=8); ny=y-18-bh
        if st!="RUNNING": arrow(c,w/2,y,w/2,y-18)
        y=ny
    y+=18+bh  # back to RUNNING y
    fy=y-45; rrect(c,cx-10,fy,bw+20,bh,4,C1,text="FINISHED",fs=9); arrow(c,w/2,y,w/2,fy+bh)
    rrect(c,cx+bw+50,y-10,bw,bh,4,CO,text="CANCELING",fs=8); arrow(c,cx+bw,y+bh/2,cx+bw+50,y-10+bh/2)
    rrect(c,cx+bw+50,fy,bw,bh,4,CO,text="CANCELED",fs=8); arrow(c,cx+bw+50+bw/2,y-10,cx+bw+50+bw/2,fy+bh)
    rrect(c,20,y-10,bw,bh,4,CR,text="FAILED",fs=8); arrow(c,cx,y+bh/2,20+bw,y-10+bh/2)

def draw_eg(c,w,h):
    c.setFont('CFB',11); c.setFillColor(CB); c.drawCentredString(w/2,h-15,"ExecutionGraph 核心层次结构")
    y1=h-50; rrect(c,w/2-80,y1,160,30,5,CB,text="ExecutionGraph",fs=10)
    y2=y1-55
    for i in range(2):
        x=60+i*240; rrect(c,x,y2,150,28,4,C1,text=f"ExecutionJobVertex {i+1}",fs=8); arrow(c,w/2,y1,x+75,y2+28)
    y3=y2-50; ps=[20,130,270,380]
    for i,px in enumerate(ps):
        rrect(c,px,y3,100,24,4,C2,text="ExecutionVertex",fs=7); arrow(c,135 if i<2 else 375,y2,px+50,y3+24)
    y4=y3-45
    for i,px in enumerate(ps):
        rrect(c,px,y4,100,24,4,CG,text="Execution",fs=7); arrow(c,px+50,y3,px+50,y4+24)
    y5=y4-45; rrect(c,w/2-80,y5,160,24,4,CO,text="IntermediateResult",fs=8); arrow(c,w/2,y4,w/2,y5+24)
    c.setFont('CF',7); c.setFillColor(CGY)
    c.drawString(20,y5-15,"1:1对应JobVertex, 包含并行度个ExecutionVertex")
    c.drawString(250,y5-15,"每个Execution代表一次执行尝试(attemptNumber递增)")

def draw_heartbeat(c,w,h):
    c.setFont('CFB',11); c.setFillColor(CB); c.drawCentredString(w/2,h-15,"心跳机制与故障检测")
    bw,bh=110,35; y=h-60
    rrect(c,30,y,bw,bh,5,C1,text="Sender(主动)\ne.g. RM",fs=7)
    rrect(c,w-30-bw,y,bw,bh,5,CG,text="Manager(被动)\ne.g. TM",fs=7)
    ym=y+bh/2
    arrow(c,30+bw,ym+5,w-30-bw,ym+5); c.setFont('CF',7); c.setFillColor(C1); c.drawCentredString(w/2,ym+12,"requestHeartbeat(id, payload)")
    arrow(c,w-30-bw,ym-5,30+bw,ym-5); c.setFont('CF',7); c.setFillColor(CG); c.drawCentredString(w/2,ym-12,"receiveHeartbeat(id, payload)")
    y2=y-55; rrect(c,w/2-70,y2,140,28,4,CO,text="HeartbeatMonitor",fs=7); darrow(c,w/2,y,w/2,y2+28)
    y3=y2-40
    for i,(s,cl) in enumerate([("RUNNING",CG),("TIMEOUT",CR),("UNREACHABLE",CGY)]):
        sx=50+i*165; rrect(c,sx,y3,120,22,4,cl,text=s,fs=8)
    arrow(c,170,y3+11,215,y3+11); arrow(c,335,y3+11,380,y3+11)
    c.setFont('CF',6); c.setFillColor(CGY); c.drawCentredString(192,y3+18,"timeout"); c.drawCentredString(357,y3+18,"N次RPC失败")

# ========== 构建文档 ==========
def build():
    register_fonts(); st=mkstyles()
    out=os.path.join(os.path.dirname(os.path.abspath(__file__)),"Flink_Runtime_核心流程与设计说明.pdf")
    doc=SimpleDocTemplate(out,pagesize=A4,topMargin=50,bottomMargin=50,leftMargin=55,rightMargin=55)
    S=[]
    # 封面
    S+=[Spacer(1,80),Paragraph("Apache Flink Runtime",st['CT']),Paragraph("核心流程与设计说明",st['CT']),Spacer(1,30)]
    S+=[Paragraph("—— 基于 Flink 1.15.4 源码深度分析 ——",st['CS']),Spacer(1,40)]
    ci=[["模块","flink-runtime"],["版本","release-1.15.4"],["内容","核心流程图 + 关键类设计说明"],["范围","调度器/Checkpoint/资源管理/网络/状态"]]
    t=Table(ci,colWidths=[80,300]); t.setStyle(TableStyle([('FONTNAME',(0,0),(0,-1),'CFB'),('FONTNAME',(1,0),(1,-1),'CF'),('FONTSIZE',(0,0),(-1,-1),11),('TEXTCOLOR',(0,0),(0,-1),C1),('TOPPADDING',(0,0),(-1,-1),6),('BOTTOMPADDING',(0,0),(-1,-1),6),('LINEBELOW',(0,0),(-1,-2),0.5,CBD)]))
    S+=[t,PageBreak()]
    # 目录
    S+=[Paragraph("目  录",st['H0']),Spacer(1,10)]
    toc=[("第一章","Flink Runtime 整体架构概览"),("  1.1","核心组件关系"),("  1.2","集群启动流程"),
         ("第二章","作业提交与调度"),("  2.1","作业提交流程"),("  2.2","调度器设计与调度策略"),("  2.3","任务部署流程"),
         ("第三章","Checkpoint 检查点机制"),("  3.1","Checkpoint 触发与执行流程"),("  3.2","Barrier 对齐机制"),("  3.3","完成与失败处理"),
         ("第四章","资源管理"),("  4.1","声明式资源管理流程"),("  4.2","Slot 分配与回收"),
         ("第五章","Task 执行与故障恢复"),("  5.1","Task 生命周期"),("  5.2","故障恢复策略"),
         ("第六章","核心类设计说明"),("  6.1","ExecutionGraph 层次结构"),("  6.2","Shuffle 与网络栈"),("  6.3","State Backend 与内存管理"),("  6.4","心跳机制")]
    for n,ti in toc:
        S.append(Paragraph(f"<b>{n}  {ti}</b>" if n.startswith("第") else f"{n}  {ti}", st['TE'] if n.startswith("第") else st['TS']))
    S.append(PageBreak())

    # 第一章
    S+=[Paragraph("第一章  Flink Runtime 整体架构概览",st['H0']),Spacer(1,8)]
    S+=[Paragraph("1.1  核心组件关系",st['H1'])]
    S+=[Paragraph("Flink Runtime 是 Apache Flink 的运行时核心，负责分布式作业的调度、执行、资源管理和容错。整个运行时由以下核心组件协作完成：",st['BD']),Spacer(1,6)]
    S.append(mktable(["组件","所在进程","核心职责"],[
        ["ClusterEntrypoint","JobManager","集群启动入口，初始化所有基础服务"],
        ["Dispatcher","JobManager","接收作业提交，管理 JobManagerRunner 生命周期"],
        ["ResourceManager","JobManager","管理集群资源(Slot)，与外部框架(YARN/K8s)交互"],
        ["JobMaster","JobManager","单个作业的调度、执行和 Checkpoint 协调"],
        ["DefaultScheduler","JobManager","作业调度策略、Task 部署和故障恢复决策"],
        ["TaskExecutor","TaskManager","管理 Slot、接收和执行 Task"],
        ["Task","TaskManager","独立线程运行用户代码（如 StreamTask）"]],cw=[100,80,300]))
    S+=[Spacer(1,8)]
    S.append(infobox("<b>设计要点：</b>Flink 采用 Master-Worker 架构。JobManager 进程包含 Dispatcher+ResourceManager+多个 JobMaster（每作业一个）；TaskManager 进程包含 TaskExecutor+多个 Task 线程。组件间通过 Akka/Pekko RPC 通信，核心方法在 ComponentMainThreadExecutor 中串行执行，避免并发问题。"))
    S+=[Spacer(1,10),Paragraph("1.2  集群启动流程",st['H1'])]
    S+=[Paragraph("Flink 集群的启动从 ClusterEntrypoint.main() 开始，经过服务初始化后创建三大核心组件：",st['BD']),Spacer(1,6)]
    S.append(FlowChart(480,310,draw_startup))
    S+=[Spacer(1,8)]
    for s in ["ClusterEntrypoint.runCluster() — 顶层入口，配置安全上下文后进入核心流程",
              "initializeServices() — 创建 RpcService、HA Services、BlobServer、HeartbeatServices 等基础设施",
              "DefaultDispatcherResourceManagerComponentFactory.create() — 创建三大核心组件",
              "DispatcherRunner 通过 Leader 选举获取 Leadership 后创建 Dispatcher 实例",
              "ResourceManagerService 启动后参与 Leader 选举，管理集群资源"]:
        S.append(Paragraph(f"• {s}",st['BL']))
    S.append(PageBreak())

    # 第二章
    S+=[Paragraph("第二章  作业提交与调度",st['H0']),Spacer(1,8)]
    S+=[Paragraph("2.1  作业提交流程",st['H1'])]
    S+=[Paragraph("客户端通过 REST API 将 JobGraph 提交给 Dispatcher，Dispatcher 持久化后创建 JobMaster 进行调度执行：",st['BD']),Spacer(1,6)]
    S.append(FlowChart(480,340,draw_submit))
    S+=[Spacer(1,8)]
    S.append(infobox("<b>关键设计：</b>作业提交时首先持久化到 HA Store（如 ZooKeeper），确保 JM 故障后可恢复未完成的作业。Dispatcher 为每个作业创建独立的 JobMasterServiceLeadershipRunner，通过 Leader 选举保证同一时刻只有一个 JobMaster 运行。"))
    S+=[Spacer(1,10),Paragraph("2.2  调度器设计与调度策略",st['H1'])]
    S+=[Paragraph("DefaultScheduler 是 Flink 核心调度器，类层次：SchedulerNG(接口) → SchedulerBase(基类) → DefaultScheduler(实现)。",st['BD']),Spacer(1,6)]
    S.append(mktable(["特性","PipelinedRegionStrategy","VertexwiseStrategy"],[
        ["调度粒度","Pipelined Region（一组顶点）","单个顶点"],
        ["适用场景","流式作业（默认）","批处理作业"],
        ["动态拓扑","不支持","支持（SchedulingTopologyListener）"],
        ["核心思想","同一Region内Task必须同时调度","逐顶点调度"]],cw=[80,200,200]))
    S+=[Spacer(1,8),Paragraph("2.3  任务部署流程",st['H1'])]
    S+=[Paragraph("DefaultExecutionDeployer 负责将调度决策转化为实际部署，分为四步：",st['BD'])]
    for s in ["<b>Step 1: validateExecutionStates()</b> — 验证所有 Execution 处于 CREATED 状态",
              "<b>Step 2: transitionToScheduled()</b> — 状态转为 SCHEDULED",
              "<b>Step 3: allocateSlotsFor()</b> — 异步分配 Slot (CompletableFuture&lt;LogicalSlot&gt;)",
              "<b>Step 4: waitForAllSlotsAndDeploy()</b> — Slot就绪后: assignResource→registerPartitions→deploy"]:
        S.append(Paragraph(f"• {s}",st['BL']))
    S.append(infobox("<b>版本控制：</b>ExecutionVertexVersioner 为每次调度/取消/故障恢复递增版本号，部署时检查版本是否过期，防止已取消的 Task 仍被部署。",CLY,CO))
    S.append(PageBreak())

    # 第三章
    S+=[Paragraph("第三章  Checkpoint 检查点机制",st['H0']),Spacer(1,8)]
    S+=[Paragraph("3.1  Checkpoint 触发与执行流程",st['H1'])]
    S+=[Paragraph("Flink 的 Checkpoint 基于 Chandy-Lamport 分布式快照算法变体。CheckpointCoordinator 是 JM 端核心协调者：",st['BD']),Spacer(1,6)]
    S.append(FlowChart(480,330,draw_checkpoint))
    S+=[Spacer(1,8),Paragraph("3.2  Barrier 对齐机制",st['H1'])]
    S.append(mktable(["模式","实现类","行为","语义"],[
        ["Exactly-Once(对齐)","SingleCheckpointBarrierHandler","阻塞已收到Barrier通道，等待收齐","精确一次"],
        ["At-Least-Once","CheckpointBarrierTracker","不阻塞通道，简单计数","至少一次"],
        ["Unaligned(非对齐)","SingleCheckpointBarrierHandler","不阻塞，将未处理数据一并快照","精确一次(低延迟)"]],cw=[100,130,150,100]))
    S+=[Spacer(1,6)]
    S.append(infobox("<b>对齐超时自动降级：</b>AlternatingCollectingBarriers 状态机支持对齐超时后自动切换非对齐模式，避免数据倾斜导致 Checkpoint 超时。",CLG,CG))
    S+=[Spacer(1,8),Paragraph("3.3  完成与失败处理",st['H1'])]
    S+=[Paragraph("所有 Task ACK 收齐后，PendingCheckpoint 升级为 CompletedCheckpoint。CheckpointFailureManager 管理失败策略：",st['BD']),Spacer(1,4)]
    S.append(mktable(["失败来源","CP类型","处理策略"],[
        ["JM级别","普通Checkpoint","计入失败计数，超阈值 failJob"],
        ["JM级别","Savepoint","不做处理（手动操作）"],
        ["TM级别","普通Checkpoint","计入失败计数"],
        ["TM级别","同步Savepoint","直接 failJob（防死锁）"]],cw=[80,120,280]))
    S+=[Spacer(1,6),Paragraph("<b>Savepoint vs Checkpoint：</b>共享底层机制但有关键差异——Savepoint 由用户手动触发/管理，始终全量快照，不可被 subsume，不添加到 CompletedCheckpointStore。",st['BD'])]
    S.append(PageBreak())

    # 第四章
    S+=[Paragraph("第四章  资源管理",st['H0']),Spacer(1,8)]
    S+=[Paragraph("4.1  声明式资源管理流程",st['H1'])]
    S+=[Paragraph("Flink 采用声明式资源管理模型。SlotPool 声明需求，SlotManager 匹配分配，ActiveRM 管理 Worker 生命周期：",st['BD']),Spacer(1,6)]
    S.append(FlowChart(480,210,draw_resource))
    S+=[Spacer(1,8)]
    S.append(mktable(["方法","调用者","职责"],[
        ["registerJobMaster()","JobMaster","JM注册：验证Leader，建立心跳"],
        ["registerTaskExecutor()","TaskExecutor","TM注册：创建WorkerRegistration"],
        ["sendSlotReport()","TaskExecutor","接收Slot状态报告"],
        ["declareRequiredResources()","JobMaster","接收资源需求声明"]],cw=[140,80,260]))
    S+=[Spacer(1,8),Paragraph("4.2  Slot 分配与回收",st['H1'])]
    S+=[Paragraph("FineGrainedSlotManager 的核心组件：TaskManagerTracker(追踪TM资源) + ResourceTracker(追踪Job需求) + ResourceAllocationStrategy(匹配策略) + SlotStatusSyncer(状态同步)。",st['BD'])]
    S.append(infobox("<b>两种模式：</b>Standalone 被动等待TM注册；Active(YARN/K8s) 通过 ResourceManagerDriver 主动申请 Worker，支持超时检查和冷却机制。",CLP,CP))
    S.append(PageBreak())

    # 第五章
    S+=[Paragraph("第五章  Task 执行与故障恢复",st['H0']),Spacer(1,8)]
    S+=[Paragraph("5.1  Task 生命周期与状态转换",st['H1'])]
    S+=[Paragraph("Task 是运行在 TaskManager 上的独立线程，通过 CAS 无锁机制保证状态转换原子性：",st['BD']),Spacer(1,6)]
    S.append(FlowChart(480,250,draw_task_lifecycle))
    S+=[Spacer(1,8)]
    S.append(mktable(["阶段","操作","说明"],[
        ["CREATED→DEPLOYING","创建类加载器","下载JAR，注册网络资源"],
        ["DEPLOYING→INIT","创建RuntimeEnv","反射创建TaskInvokable(如StreamTask)"],
        ["restore()","恢复状态","从Checkpoint恢复KeyedState和OperatorState"],
        ["INIT→RUNNING","invoke()","执行用户代码主循环"],
        ["RUNNING→终态","完成/失败/取消","释放资源，通知TaskManager最终状态"]],cw=[100,100,280]))
    S+=[Spacer(1,10),Paragraph("5.2  故障恢复策略",st['H1']),Spacer(1,6)]
    S.append(FlowChart(480,210,draw_failover))
    S+=[Spacer(1,8)]
    for r in ["<b>规则1</b>：失败Task所在Region必须重启",
              "<b>规则2</b>：如果Region的输入分区不可用，生产该分区的Region也需重启",
              "<b>规则3</b>：如果一个Region需重启，其所有下游消费者Region也必须重启"]:
        S.append(Paragraph(f"• {r}",st['BL']))
    S.append(infobox("<b>状态恢复：</b>全局恢复调用 restoreLatestCheckpointedStateToAll()；局部恢复调用 restoreLatestCheckpointedStateToSubtasks()，仅恢复受影响的 Task。",CLG,CG))
    S.append(PageBreak())

    # 第六章
    S+=[Paragraph("第六章  核心类设计说明",st['H0']),Spacer(1,8)]
    S+=[Paragraph("6.1  ExecutionGraph 层次结构",st['H1']),Spacer(1,6)]
    S.append(FlowChart(480,260,draw_eg))
    S+=[Spacer(1,8)]
    S.append(mktable(["类名","对应关系","核心职责"],[
        ["DefaultExecutionGraph","整个作业","维护顶点、中间结果、作业状态、CheckpointCoordinator"],
        ["ExecutionJobVertex","1:1对应JobVertex","一个算子逻辑表示，含并行度个ExecutionVertex"],
        ["ExecutionVertex","一个并行子任务","管理当前Execution、历史记录和输出分区"],
        ["Execution","一次执行尝试","状态转换、部署、取消，绑定LogicalSlot"],
        ["IntermediateResult","算子间数据集","上下游算子间传递的中间数据"]],cw=[120,90,270]))
    S+=[Spacer(1,8)]
    S.append(infobox("<b>两层状态管理：</b>JM侧(Execution)使用主线程串行更新，天然单线程保证；TM侧(Task)使用AtomicReferenceFieldUpdater CAS无锁更新，允许执行线程和取消线程并发修改。"))
    S+=[Spacer(1,10),Paragraph("6.2  Shuffle 与网络栈",st['H1'])]
    S.append(mktable(["组件","所在端","核心职责"],[
        ["ShuffleMaster","JobMaster","分区注册、生成ShuffleDescriptor（连接信息）"],
        ["ShuffleEnvironment","TaskManager","创建ResultPartitionWriter和InputGate"],
        ["NetworkBufferPool","TaskManager","全局共享内存缓冲池（堆外，默认32KB/页）"],
        ["ResultPartition","TaskManager","数据输出(Pipelined/Blocking/SortMerge/Hybrid)"],
        ["SingleInputGate","TaskManager","数据输入(Local/Remote/UnknownChannel)"],
        ["ConnectionManager","TaskManager","基于Netty的网络连接管理"]],cw=[110,80,290]))
    S+=[Spacer(1,6),Paragraph("<b>Credit-based流控：</b>下游通过notifyCreditAvailable()告知上游可用缓冲区数量，上游仅在有信用时发送数据，实现端到端反压传播。",st['BD'])]
    S+=[Spacer(1,10),Paragraph("6.3  State Backend 与内存管理",st['H1'])]
    S.append(mktable(["分类","实现","存储位置","适用场景"],[
        ["KeyedStateBackend","HeapKeyedStateBackend","JVM堆内存","状态较小，低延迟"],
        ["KeyedStateBackend","RocksDBKeyedStateBackend","本地RocksDB","TB级大状态，支持增量CP"],
        ["CheckpointStorage","JobManagerCPStorage","JM内存","仅测试"],
        ["CheckpointStorage","FileSystemCPStorage","HDFS/S3","生产推荐"]],cw=[100,130,100,150]))
    S+=[Spacer(1,6)]
    for f in ["<b>预算控制</b>：UnsafeMemoryBudget用CAS原子操作管理内存预算，防OOM",
              "<b>两种分配</b>：MemorySegment(固定32KB页)和Chunk预留(RocksDB)",
              "<b>共享资源</b>：SharedResources支持多算子共享内存（引用计数管理）",
              "<b>Owner机制</b>：内存绑定Owner，支持一次性释放和泄漏检测"]:
        S.append(Paragraph(f"• {f}",st['BL']))
    S+=[Spacer(1,10),Paragraph("6.4  心跳机制",st['H1']),Spacer(1,6)]
    S.append(FlowChart(480,200,draw_heartbeat))
    S+=[Spacer(1,8)]
    S.append(mktable(["心跳关系","主动方(Sender)","被动方","用途"],[
        ["RM↔TM","ResourceManager","TaskManager","资源可用性监控，携带SlotReport"],
        ["JM↔TM","JobMaster","TaskManager","Task状态监控，携带累加器快照"],
        ["RM↔JM","ResourceManager","JobMaster","作业健康监控"]],cw=[70,110,100,200]))
    S+=[Spacer(1,6)]
    S.append(infobox("<b>双重故障检测：</b>DefaultHeartbeatMonitor同时支持时间超时(heartbeat.timeout=50s)和连续RPC失败两种故障检测方式，超时→TIMEOUT状态，N次RPC失败→UNREACHABLE状态。"))

    doc.build(S,onFirstPage=hf,onLaterPages=hf)
    print(f"\n✅ PDF 已生成: {out}")

if __name__=='__main__':
    build()
