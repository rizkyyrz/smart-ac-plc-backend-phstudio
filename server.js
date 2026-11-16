/**
 * Smart AC by RZKY Studio — educational PLC simulator
 * WebSocket is READ-ONLY. Control actions stay behind Supabase Auth + protected RPC.
 * All temperatures and power estimates below are VIRTUAL and not physical sensor readings.
 */
import {createServer} from 'node:http';
import {WebSocketServer, WebSocket} from 'ws';
import {createClient} from '@supabase/supabase-js';

const PORT = Number(process.env.PORT || 10000);
const SUPABASE_URL = process.env.SUPABASE_URL?.trim();
const SERVER_KEY = (process.env.SUPABASE_SECRET_KEY || process.env.SUPABASE_SERVICE_ROLE_KEY || '').trim();
const db = SUPABASE_URL && SERVER_KEY
  ? createClient(SUPABASE_URL, SERVER_KEY, {auth:{persistSession:false,autoRefreshToken:false}})
  : null;
const units = new Map();
let connected = false, ambientTemp = 31, lastPoll = null;
const round = (x,d=2)=>Number(x.toFixed(d));
const fanLevels = {LOW:1, MED:2, HIGH:3, AUTO:2};
function snapshot(u) {
  return {
    unitId:u.id,roomCode:u.roomCode, label:u.label,
    power:u.power,autoMode:u.autoMode,compressor:u.compressor,swing:u.swing,
    fanSpeed:u.fanSpeed,fanMode:u.fanMode,
    setpoint:u.setpoint,roomTemp:round(u.roomTemp),ambientTemp:round(ambientTemp,1),
    powerKw:round(u.powerKw),alarmHighTemp:u.alarmHighTemp,
    registers:{'%Q0.0':u.power?1:0,'%M0.0':u.autoMode?1:0,'%Q0.1':u.compressor?1:0,
      '%Q0.2':u.swing?1:0,'%Q0.3':u.alarmHighTemp?1:0,
      '%MW10':u.setpoint,'%MW12':round(u.roomTemp),'%MW16':round(u.powerKw),'%MW20':u.fanSpeed}
  };
}
function packet(){return JSON.stringify({type:'PLC_STATE',data:[...units.values()].map(snapshot),time:new Date().toISOString(),
  connectedToDatabase:connected,virtual:true,scanIntervalMs:1000});}
const server = createServer((req,res)=>{
  if(req.url === '/health') {
    res.writeHead(connected?200:503,{'Content-Type':'application/json','Cache-Control':'no-store'});
    res.end(JSON.stringify({status:connected?'ok':db?'waiting_for_database':'missing_environment_variables',
      unitCount:units.size,connectedToDatabase:connected,lastPoll,virtual:true}));
    return;
  }
  res.writeHead(200,{'Content-Type':'text/plain; charset=utf-8'});
  res.end('Smart AC by RZKY Studio — PLC Simulation API. WebSocket: /ws; Health: /health');
});
const wss = new WebSocketServer({server,path:'/ws',maxPayload:512});
wss.on('connection',socket=>{
  socket.isAlive=true;
  socket.on('pong',()=>{socket.isAlive=true});
  socket.on('message',raw=>{
    // Only keepalive supported; reject every attempt to control from public WebSocket.
    if(raw.length<256 && String(raw).includes('PING')) socket.send(JSON.stringify({type:'PONG'}));
  });
  socket.on('error',()=>{});
  socket.send(packet());
});
setInterval(()=>{for(const ws of wss.clients){if(!ws.isAlive){ws.terminate();continue;}ws.isAlive=false;ws.ping();}},30000).unref();
function broadcast(){const data=packet();for(const c of wss.clients){if(c.readyState===WebSocket.OPEN)c.send(data)}}
function applyUnit(row){
  let u=units.get(row.id);
  if(!u){
    // Start from a warm indoor temperature, and cool down as the PLC runs.
    u={id:row.id,roomCode:row.room_code,label:row.label,roomTemp:29.2,compressor:false,alarmHighTemp:false,powerKw:0};
    units.set(row.id,u);
  }
  u.power=!!row.power_on;
  u.autoMode=row.control_mode==='AUTO';
  u.swing=!!row.swing_on;
  u.setpoint=Number(row.target_temp)||24;
  u.fanMode=row.fan_mode||'AUTO';
  u.fanSpeed=fanLevels[u.fanMode]||2;
  u.hvacMode=row.hvac_mode||'COOL';
}
async function pollDatabase(){
  if(!db)return;
  try{
    const {data,error}=await db.from('ac_units').select('id,room_code,label,power_on,control_mode,swing_on,target_temp,fan_mode,hvac_mode');
    if(error)throw error;
    const ids=new Set(data.map(x=>x.id));
    for(const u of data)applyUnit(u);
    for(const id of units.keys())if(!ids.has(id))units.delete(id);
    connected=true;lastPoll=new Date().toISOString();
  }catch(e){connected=false;console.error('Database poll failed:',e.message)}
}
async function weatherSync(){
  try{
    const ctl=new AbortController();const timer=setTimeout(()=>ctl.abort(),6000);
    let res;try {res=await fetch('https://api.open-meteo.com/v1/forecast?latitude=-6.1177&longitude=106.1539&current=temperature_2m&timezone=Asia%2FJakarta',{signal:ctl.signal})}finally{clearTimeout(timer)}
    if(!res.ok)throw Error('Weather '+res.status);
    const d=await res.json();const t=Number(d?.current?.temperature_2m);
    if(Number.isFinite(t))ambientTemp=t;
  }catch(e){console.warn('Weather unavailable; using last ambient value:',e.message)}
}
function scan(){
  for(const u of units.values()){
    if(!u.power || u.hvacMode==='FAN')u.compressor=false;
    else if(u.autoMode){
      // Auto thermostat hysteresis: ON above setpoint + 0.4, OFF at/below -0.4.
      if(u.roomTemp>u.setpoint+0.4)u.compressor=true;
      else if(u.roomTemp<=u.setpoint-0.4)u.compressor=false;
    }else u.compressor=true; // Manual switch keeps cooling active while powered.
    if(u.power){
      if(u.compressor){const rate=u.fanSpeed===3?.35:u.fanSpeed===2?.25:.15;
        u.roomTemp=Math.max(16,round(u.roomTemp-rate));
        u.powerKw=round(.95+u.fanSpeed*.22);
      }else{
        u.roomTemp=Math.min(ambientTemp,round(u.roomTemp+.07));
        u.powerKw=round(.08+u.fanSpeed*.03);
      }
    }else{
      u.powerKw=0;
      if(u.roomTemp<ambientTemp)u.roomTemp=Math.min(ambientTemp,round(u.roomTemp+.12));
      else if(u.roomTemp>ambientTemp)u.roomTemp=Math.max(ambientTemp,round(u.roomTemp-.05));
    }
    u.alarmHighTemp=u.roomTemp>=30;
  }
  broadcast();
}
async function persistTelemetry(){
  if(!db||!connected||units.size===0)return;
  try{
    const records=[...units.values()].map(u=>({unit_id:u.id,room_temp:round(u.roomTemp),compressor:u.compressor,
      alarm_high_temp:u.alarmHighTemp,power_kw:round(u.powerKw),ambient_temp:round(ambientTemp,1),
      updated_at:new Date().toISOString()}));
    const {error}=await db.from('plc_telemetry').upsert(records,{onConflict:'unit_id'});
    if(error)throw error;
  }catch(e){console.error('Telemetry save failed:',e.message)}
}
async function restoreTelemetry(){
  if(!db)return;
  try{
    const {data,error}=await db.from('plc_telemetry').select('unit_id,room_temp,compressor');
    if(error)throw error;
    for(const x of data){const u=units.get(x.unit_id);if(u){u.roomTemp=Number(x.room_temp);u.compressor=!!x.compressor}}
  }catch(e){console.warn('Restore telemetry skipped:',e.message)}
}
server.listen(PORT,'0.0.0.0',async()=>{
  console.log(`PLC simulator running on port ${PORT}; database ${db?'configured':'NOT configured'}`);
  await pollDatabase();await restoreTelemetry();await weatherSync();
  setInterval(scan,1000);
  setInterval(pollDatabase,2500);
  setInterval(persistTelemetry,30000);
  setInterval(weatherSync,15*60*1000);
});
