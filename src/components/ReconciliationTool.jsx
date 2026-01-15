import React, { useState, useMemo, useEffect } from 'react';
import { 
  CheckCircle, AlertCircle, Printer, ChevronDown, Play, XCircle, 
  Unlink, BookmarkCheck, Banknote, RotateCcw, Calendar, HardDriveDownload 
} from 'lucide-react';
import Papa from 'papaparse';
import * as XLSX from 'xlsx';
import { createClient } from '@supabase/supabase-js';

const supabase = createClient(
  import.meta.env.VITE_SUPABASE_URL,
  import.meta.env.VITE_SUPABASE_ANON_KEY
);

const TOLERANCIA = 0.05;

export default function ReconciliationTool() {
  const [invoices, setInvoices] = useState([]);
  const [bankData, setBankData] = useState([]);
  const [matches, setMatches] = useState({}); 
  const [bankExclusions, setBankExclusions] = useState(new Set()); 
  const [invoiceCash, setInvoiceCash] = useState(new Set()); 

  const [selectedQuarters, setSelectedQuarters] = useState([1, 2, 3, 4]);
  const [selectedYear, setSelectedYear] = useState(new Date().getFullYear());
  const [selectedInvIndices, setSelectedInvIndices] = useState(new Set());
  const [selectedBankIdx, setSelectedBankIdx] = useState(null);
  const [showPaired, setShowPaired] = useState(true);
  const [loading, setLoading] = useState(false);

  const COL_FAC_DATA = "DATA";
  const COL_FAC_NUM = "ULTIMA 4 DIGITS NUMERO FACTURA";
  const COL_FAC_PROV = "PROVEEDOR";
  const COL_FAC_TOTAL = "TOTAL FACTURA";
  
  const COL_BANK_DATA = "F. Operativa";
  const COL_BANK_DESC = "Concepto";
  const COL_BANK_IMPORT = "Importe";

  // HASH per identificar registres
  const getRecordHash = (item, tipus) => {
    try {
      const dateStr = tipus === 'factura' ? item[COL_FAC_DATA] : item[COL_BANK_DATA];
      const amount = tipus === 'factura' ? item[COL_FAC_TOTAL] : item[COL_BANK_IMPORT];
      const desc = tipus === 'factura' ? item[COL_FAC_PROV] : item[COL_BANK_DESC];
      const str = `${tipus}|${dateStr}|${amount}|${desc}`.substring(0, 200);
      return btoa(encodeURIComponent(str)).substring(0, 100);
    } catch (e) {
      console.error('Error creant hash:', e);
      return Math.random().toString(36);
    }
  };

  // GUARDAR múltiples conciliacions en batch
  const saveConciliacionsEnMasa = async (conciliacions) => {
    if (conciliacions.length === 0) return true;
    
    console.log('💾 Intentant guardar:', conciliacions.length, 'conciliacions');
    
    try {
      // Validar i eliminar duplicats
      const uniqueMap = new Map();
      
      conciliacions.forEach(c => {
        if (c.factura_hash && c.tipus_conciliacio) {
          // Usar factura_hash com a clau única
          uniqueMap.set(c.factura_hash, c);
        } else {
          console.warn('⚠️ Conciliació invàlida ignorada:', c);
        }
      });

      const validConciliacions = Array.from(uniqueMap.values());

      if (validConciliacions.length === 0) {
        console.error('❌ Cap conciliació vàlida!');
        return false;
      }

      if (validConciliacions.length < conciliacions.length) {
        console.warn(`⚠️ ${conciliacions.length - validConciliacions.length} duplicats eliminats`);
      }

      console.log('✅ Conciliacions úniques:', validConciliacions.length);
      console.log('📦 Primera conciliació:', validConciliacions[0]);

      // Guardar en batch
      const { data, error } = await supabase
        .from('conciliacions')
        .upsert(validConciliacions, { 
          onConflict: 'factura_hash',
          ignoreDuplicates: false 
        });
      
      if (error) {
        console.error('❌ Error de Supabase:', error);
        throw error;
      }
      
      console.log(`✅ ${validConciliacions.length} conciliacions guardades correctament`);
      console.log('📊 Resposta:', data);
      return true;
    } catch (error) {
      console.error('❌ Error guardant conciliacions:', error);
      
      // Si encara falla, intentar una a una
      console.log('🔄 Intentant guardar una a una...');
      let success = 0;
      let failed = 0;
      
      const uniqueMap = new Map();
      conciliacions.forEach(c => {
        if (c.factura_hash && c.tipus_conciliacio) {
          uniqueMap.set(c.factura_hash, c);
        }
      });
      const validConciliacions = Array.from(uniqueMap.values());
      
      for (const conc of validConciliacions) {
        try {
          await supabase.from('conciliacions').upsert([conc], { 
            onConflict: 'factura_hash' 
          });
          success++;
        } catch (e) {
          console.warn('⚠️ Error guardant:', conc, e.message);
          failed++;
        }
      }
      
      console.log(`📊 Resultat: ${success} guardades, ${failed} fallides`);
      
      if (success > 0) {
        alert(`⚠️ Només s'han pogut guardar ${success} de ${validConciliacions.length} conciliacions`);
        return true;
      }
      
      alert(`❌ Error: ${error.message || 'Error desconegut'}`);
      return false;
    }
  };

  // GUARDAR UNA conciliació
  const saveConciliacio = async (facturaHash, bancHash = null, tipus = 'banc') => {
    try {
      const { error } = await supabase.from('conciliacions').upsert(
        [{
          factura_hash: facturaHash,
          banc_hash: bancHash,
          tipus_conciliacio: tipus
        }],
        { onConflict: 'factura_hash' }
      );
      
      if (error) throw error;
      console.log('✅ Conciliació guardada:', tipus);
      return true;
    } catch (error) {
      console.error('❌ Error guardant conciliació:', error);
      return false;
    }
  };

  // ELIMINAR conciliació
  const deleteConciliacio = async (facturaHash) => {
    try {
      const { error } = await supabase
        .from('conciliacions')
        .delete()
        .eq('factura_hash', facturaHash);
      
      if (error) throw error;
      console.log('✅ Conciliació eliminada');
      return true;
    } catch (error) {
      console.error('❌ Error eliminant:', error);
      return false;
    }
  };

  // GUARDAR registre a Supabase
  const syncSupabase = async (tipus, item) => {
    const dateStr = tipus === 'factura' ? item[COL_FAC_DATA] : item[COL_BANK_DATA];
    const year = getYear(dateStr);
    const quarter = getQuarter(dateStr);
    
    if (year > 2000) {
      const uniqueHash = getRecordHash(item, tipus);
      
      try {
        const { data, error } = await supabase.from('registres_comptables').upsert(
          [{ 
            tipus, 
            contingut: item, 
            ejercicio: year, 
            trimestre: quarter,
            unique_hash: uniqueHash
          }],
          { onConflict: 'unique_hash' }
        );
        
        if (error && error.code !== '23505') {
          console.error('❌ Error guardant registre:', error);
          throw error;
        }
        
        return true;
      } catch (error) {
        if (error.code !== '23505') { // Ignorar duplicats
          console.error('❌ Error guardant registre a Supabase:', {
            tipus,
            error: error.message,
            item: { 
              data: dateStr, 
              desc: tipus === 'factura' ? item[COL_FAC_PROV] : item[COL_BANK_DESC] 
            }
          });
          throw error;
        }
        return true; // Duplicat, però és OK
      }
    } else {
      console.warn('⚠️ Any invàlid, no es guarda:', year, dateStr);
      return false;
    }
  };

  // CARREGAR dades i conciliacions
  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      try {
        // Carregar registres
        const { data: registres, error: errorRegistres } = await supabase
          .from('registres_comptables')
          .select('*');
        
        if (errorRegistres) throw errorRegistres;
        
        // Carregar conciliacions
        const { data: conciliacions, error: errorConc } = await supabase
          .from('conciliacions')
          .select('*');
        
        if (errorConc) throw errorConc;
        
        if (registres) {
          const facturesData = registres.filter(d => d.tipus === 'factura');
          const bancData = registres.filter(d => d.tipus === 'banc');
          
          const invoicesList = facturesData.map(d => d.contingut);
          const bankList = bancData.map(d => d.contingut);
          
          setInvoices(invoicesList);
          setBankData(bankList);
          
          // Crear mapa de hash -> índex
          const invHashToIdx = new Map();
          invoicesList.forEach((inv, idx) => {
            invHashToIdx.set(getRecordHash(inv, 'factura'), idx);
          });
          
          const bankHashToIdx = new Map();
          bankList.forEach((bank, idx) => {
            bankHashToIdx.set(getRecordHash(bank, 'banc'), idx);
          });
          
          // Reconstruir estat
          const newMatches = {};
          const newCash = new Set();
          const newExclusions = new Set();
          
          if (conciliacions) {
            console.log(`📥 Carregant ${conciliacions.length} conciliacions...`);
            
            conciliacions.forEach(conc => {
              if (conc.tipus_conciliacio === 'banc' && conc.banc_hash) {
                // Conciliació banc-factura
                const invIdx = invHashToIdx.get(conc.factura_hash);
                const bankIdx = bankHashToIdx.get(conc.banc_hash);
                
                if (invIdx !== undefined && bankIdx !== undefined) {
                  newMatches[invIdx] = bankIdx;
                }
              } else if (conc.tipus_conciliacio === 'cash') {
                // Pagament en cash
                const invIdx = invHashToIdx.get(conc.factura_hash);
                if (invIdx !== undefined) {
                  newCash.add(invIdx);
                }
              } else if (conc.tipus_conciliacio === 'exclos') {
                // Moviment bancari sense factura - CORRECCIÓ IMPORTANT!
                // Ara guardem el hash del banc al camp factura_hash
                const bankIdx = bankHashToIdx.get(conc.factura_hash);
                if (bankIdx !== undefined) {
                  newExclusions.add(bankIdx);
                }
              }
            });
          }
          
          setMatches(newMatches);
          setInvoiceCash(newCash);
          setBankExclusions(newExclusions);
          
          console.log('📊 Estat carregat:', {
            factures: invoicesList.length,
            banc: bankList.length,
            matches: Object.keys(newMatches).length,
            cash: newCash.size,
            exclusions: newExclusions.size
          });
        }
      } catch (error) {
        console.error('❌ Error carregant dades:', error);
        alert('Error carregant dades de Supabase. Comprova la connexió.');
      } finally {
        setLoading(false);
      }
    };
    fetchData();
  }, []);

  const getQuarter = (d) => {
    if (!d || typeof d !== 'string' || (!d.includes('/') && !d.includes('-'))) return -1;
    const cleanD = d.trim().replace(/-/g, '/');
    const parts = cleanD.split('/');
    return Math.ceil(parseInt(parts[1]) / 3) || -1;
  };

  const getYear = (d) => {
    if (!d || typeof d !== 'string' || (!d.includes('/') && !d.includes('-'))) return -1;
    const cleanD = d.trim().replace(/-/g, '/');
    const parts = cleanD.split('/');
    if (parts.length < 3) return -1;
    let y = parseInt(parts[2]);
    return y < 100 ? 2000 + y : y;
  };

  const parseAmount = (v) => {
    if (!v) return 0;
    let s = String(v).trim().replace(/[^\d,.-]/g, '');
    if (s.includes(',') && s.includes('.')) s = s.replace(/\./g, '').replace(',', '.');
    else if (s.includes(',')) s = s.replace(',', '.');
    return Math.abs(parseFloat(s)) || 0;
  };

  const filteredInvoices = useMemo(() => {
    return invoices.filter(inv => {
      const d = inv[COL_FAC_DATA];
      return getYear(d) === selectedYear && selectedQuarters.includes(getQuarter(d));
    });
  }, [invoices, selectedQuarters, selectedYear]);

  const filteredBank = useMemo(() => {
    return bankData.filter(row => {
      const d = row[COL_BANK_DATA];
      return getYear(d) === selectedYear && selectedQuarters.includes(getQuarter(d));
    });
  }, [bankData, selectedQuarters, selectedYear]);

  const filteredMatchesList = useMemo(() => {
    return Object.entries(matches).filter(([invIdx]) => {
      const d = invoices[invIdx]?.[COL_FAC_DATA];
      return d && getYear(d) === selectedYear && selectedQuarters.includes(getQuarter(d));
    });
  }, [matches, invoices, selectedYear, selectedQuarters]);

  const filteredCashList = useMemo(() => {
    return [...invoiceCash].filter(invIdx => {
      const d = invoices[invIdx]?.[COL_FAC_DATA];
      return d && getYear(d) === selectedYear && selectedQuarters.includes(getQuarter(d));
    });
  }, [invoiceCash, invoices, selectedYear, selectedQuarters]);

  // NOUS COMPTADORS - FACTURES
  const invoiceStats = useMemo(() => {
    const conciliades = filteredInvoices.filter((inv, i) => {
      const rIdx = invoices.indexOf(inv);
      return matches[rIdx] !== undefined || invoiceCash.has(rIdx);
    }).length;
    
    return {
      totals: filteredInvoices.length,
      conciliades: conciliades,
      pendents: filteredInvoices.length - conciliades
    };
  }, [filteredInvoices, invoices, matches, invoiceCash]);

  // NOUS COMPTADORS - BANC
  const bankStats = useMemo(() => {
    const conciliades = filteredBank.filter((row, i) => {
      const rIdx = bankData.indexOf(row);
      return Object.values(matches).includes(rIdx) || bankExclusions.has(rIdx);
    }).length;
    
    return {
      totals: filteredBank.length,
      conciliades: conciliades,
      pendents: filteredBank.length - conciliades
    };
  }, [filteredBank, bankData, matches, bankExclusions]);

  // FILTRAR MOVIMENTS BANCARIS SENSE FACTURA (SF)
  const filteredBankExclusionsList = useMemo(() => {
    return [...bankExclusions].filter(bankIdx => {
      const d = bankData[bankIdx]?.[COL_BANK_DATA];
      return d && getYear(d) === selectedYear && selectedQuarters.includes(getQuarter(d));
    });
  }, [bankExclusions, bankData, selectedYear, selectedQuarters]);

  // CONCILIACIÓ AUTOMÀTICA OPTIMITZADA
  const handleAutoReconcile = async () => {
    setLoading(true);
    const startTime = Date.now();
    
    try {
      const newMatches = { ...matches };
      const usedBank = new Set([...Object.values(matches), ...bankExclusions]);
      const conciliacionsPerGuardar = [];
      let trobadesNoves = 0;

      // Trobar parelles
      for (const inv of filteredInvoices) {
        const rIdx = invoices.indexOf(inv);
        if (newMatches[rIdx] || invoiceCash.has(rIdx)) continue;
        
        const amt = parseAmount(inv[COL_FAC_TOTAL]);
        const bIdx = bankData.findIndex((b, j) => 
          !usedBank.has(j) && 
          Math.abs(amt - parseAmount(b[COL_BANK_IMPORT])) <= TOLERANCIA
        );
        
        if (bIdx !== -1) {
          const invAmt = parseAmount(inv[COL_FAC_TOTAL]);
          const bankAmt = parseAmount(bankData[bIdx][COL_BANK_IMPORT]);
          const diff = Math.abs(invAmt - bankAmt);
          
          console.log('🔗 Parella trobada:', {
            factura: `${inv[COL_FAC_PROV]} - ${invAmt}€`,
            banc: `${bankData[bIdx][COL_BANK_DESC]} - ${bankAmt}€`,
            diferencia: diff
          });
          
          newMatches[rIdx] = bIdx;
          usedBank.add(bIdx);
          trobadesNoves++;
          
          // Preparar per guardar
          const invHash = getRecordHash(inv, 'factura');
          const bankHash = getRecordHash(bankData[bIdx], 'banc');
          conciliacionsPerGuardar.push({
            factura_hash: invHash,
            banc_hash: bankHash,
            tipus_conciliacio: 'banc'
          });
        }
      }

      // Guardar TOTES les conciliacions d'un cop
      if (conciliacionsPerGuardar.length > 0) {
        const success = await saveConciliacionsEnMasa(conciliacionsPerGuardar);
        if (!success) {
          alert('⚠️ Error guardant algunes conciliacions. Torna-ho a provar.');
          setLoading(false);
          return;
        }
      }

      setMatches(newMatches);

      const totalVisibles = filteredInvoices.length;
      const jaConciliades = filteredInvoices.filter(inv => {
          const idx = invoices.indexOf(inv);
          return newMatches[idx] !== undefined || invoiceCash.has(idx);
      }).length;

      const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
      
      alert(
        `✅ CONCILIACIÓ COMPLETADA en ${elapsed}s\n\n` +
        `🆕 Noves parelles: ${trobadesNoves}\n` +
        `📊 Total conciliades: ${jaConciliades}\n` +
        `⏳ Pendents: ${totalVisibles - jaConciliades}`
      );
    } catch (error) {
      console.error('Error en conciliació:', error);
      alert('❌ Error durant la conciliació. Comprova la consola.');
    } finally {
      setLoading(false);
    }
  };

  const handleCSV = (e) => {
    setLoading(true);
    Papa.parse(e.target.files[0], {
      header: true, delimiter: ";", skipEmptyLines: true, transformHeader: h => h.trim(),
      complete: async (r) => {
        try {
          console.log('📥 Processant CSV amb', r.data.length, 'registres');
          
          const news = r.data.map(obj => {
            Object.keys(obj).forEach(k => obj[k] = typeof obj[k] === 'string' ? obj[k].trim() : obj[k]);
            return obj;
          });
          
          // Guardar TOTS els registres a Supabase en paral·lel
          console.log('💾 Guardant factures a Supabase...');
          const promises = news.map(obj => syncSupabase('factura', obj));
          await Promise.all(promises);
          
          console.log('✅ Factures guardades correctament');
          setInvoices(prev => [...prev, ...news]);
          
          alert(`✅ ${news.length} factures carregades i guardades a Supabase!`);
        } catch (error) {
          console.error('❌ Error guardant factures:', error);
          alert('❌ Error guardant les dades a Supabase. Comprova la connexió.');
        } finally {
          setLoading(false);
        }
      }
    });
  };

  const handleExcel = (e) => {
    setLoading(true);
    const reader = new FileReader();
    reader.onload = async (evt) => {
      try {
        const wb = XLSX.read(new Uint8Array(evt.target.result), { type: 'array' });
        const rows = XLSX.utils.sheet_to_json(wb.Sheets[wb.SheetNames[0]], { header: 1, defval: "" });
        
        console.log('📄 Total files llegides:', rows.length);
        
        // MILLORA: Buscar la fila que conté les capçaleres
        // Busquem la fila que conté "F. Operativa", "Concepto", "Importe"
        const hIdx = rows.findIndex(r => {
          const rowStr = r.map(c => String(c).trim().toLowerCase()).join('|');
          return rowStr.includes('f. operativa') || rowStr.includes('f.operativa');
        });
        
        console.log('📍 Fila amb capçaleres trobada a:', hIdx);
        
        if (hIdx !== -1) {
          const h = rows[hIdx].map(x => String(x).trim());
          console.log('📋 Capçaleres detectades:', h);
          
          // Processar només les files de dades (després de la capçalera)
          const dataRows = rows.slice(hIdx + 1);
          console.log('📊 Files de dades a processar:', dataRows.length);
          
          const news = dataRows
            .filter(r => {
              // Filtrar files buides i files sense data vàlida
              const hasContent = r.some(c => String(c).trim() !== "");
              const firstCol = String(r[0] || "").trim();
              
              // Comprovar si la primera columna sembla una data (format DD/MM/YYYY)
              const isDate = /^\d{1,2}\/\d{1,2}\/\d{4}$/.test(firstCol);
              
              return hasContent && isDate;
            })
            .map(r => {
              const obj = {}; 
              h.forEach((col, i) => {
                if (col) { // Només processar columnes amb nom
                  obj[col] = String(r[i] || "").trim();
                }
              });
              
              // Verificar que té les columnes essencials
              if (obj[COL_BANK_DATA] && obj[COL_BANK_DESC]) {
                console.log('✅ Registre vàlid:', {
                  data: obj[COL_BANK_DATA],
                  desc: obj[COL_BANK_DESC]?.substring(0, 30) + '...',
                  import: obj[COL_BANK_IMPORT]
                });
                return obj;
              }
              return null;
            })
            .filter(obj => obj !== null);
          
          console.log('✅ Registres bancaris processats:', news.length);
          
          if (news.length === 0) {
            alert('⚠️ No s\'han pogut carregar dades del banc. Comprova el format del fitxer.');
            setLoading(false);
            return;
          }
          
          // Guardar TOTS els registres a Supabase en paral·lel
          console.log('💾 Guardant moviments bancaris a Supabase...');
          const promises = news.map(obj => syncSupabase('banc', obj));
          await Promise.all(promises);
          
          console.log('✅ Moviments bancaris guardats correctament a Supabase');
          setBankData(prev => [...prev, ...news]);
          alert(`✅ ${news.length} moviments bancaris carregats i guardats a Supabase!`);
        } else {
          console.error('❌ No s\'ha trobat la fila de capçaleres');
          alert('❌ No s\'ha pogut trobar les capçaleres al fitxer Excel. Comprova que conté "F. Operativa", "Concepto" i "Importe".');
        }
      } catch (error) {
        console.error('❌ Error processant Excel:', error);
        alert('❌ Error processant el fitxer Excel: ' + error.message);
      } finally {
        setLoading(false);
      }
    };
    reader.readAsArrayBuffer(e.target.files[0]);
  };

  const printSection = (elementId, title) => {
    const content = document.getElementById(elementId).cloneNode(true);
    content.querySelectorAll('.no-print').forEach(el => el.remove());
    const pri = document.getElementById("ifmcontentstoprint").contentWindow;
    pri.document.open();
    pri.document.write(`<html><head><link href="https://cdn.tailwindcss.com" rel="stylesheet"><style>@page { size: portrait; margin: 1cm; } body { font-size: 7pt; font-family: sans-serif; padding: 10px; } table { width: 100%; border-collapse: collapse; } th, td { border: 1px solid #000; padding: 3px; }</style></head><body><h1 class="text-lg font-bold mb-4 border-b-2 pb-2">${title} (${selectedYear})</h1>${content.innerHTML}</body></html>`);
    pri.document.close();
    setTimeout(() => { pri.focus(); pri.print(); }, 700);
  };

  const totalSelected = useMemo(() => {
    let sum = 0; 
    selectedInvIndices.forEach(idx => sum += parseAmount(invoices[idx]?.[COL_FAC_TOTAL]));
    return sum;
  }, [selectedInvIndices, invoices]);

  return (
    <div className="w-full min-h-screen bg-slate-100 p-2 font-sans text-[11px]">
      {loading && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-[100]">
          <div className="bg-white p-8 rounded-2xl shadow-2xl flex flex-col items-center gap-4">
            <div className="animate-spin rounded-full h-16 w-16 border-b-4 border-indigo-600"></div>
            <p className="text-lg font-bold text-gray-700">Processant...</p>
          </div>
        </div>
      )}
      
      <iframe id="ifmcontentstoprint" className="hidden" title="print"></iframe>

      {/* HEADER FIXA */}
      <div className="sticky top-0 z-50 w-full bg-white p-4 rounded-b-xl shadow-md mb-4 border-b flex items-center gap-4 no-print flex-wrap">
        <h1 className="text-xl font-black text-indigo-700 italic border-r pr-4 uppercase tracking-tighter">FinMatch v11</h1>
        
        <div className="flex gap-1 bg-slate-50 p-1 rounded-lg border items-center">
          <Calendar size={14} className="text-slate-400 mx-1"/>
          <select value={selectedYear} onChange={e => setSelectedYear(parseInt(e.target.value))} className="font-bold border-none bg-transparent outline-none cursor-pointer">
            {[2024, 2025, 2026].map(y => <option key={y} value={y}>{y}</option>)}
          </select>
          <div className="h-4 w-px bg-slate-300 mx-1"></div>
          {[1, 2, 3, 4].map(q => (
            <button key={q} onClick={() => setSelectedQuarters(prev => prev.includes(q) ? prev.filter(x => x !== q) : [...prev, q])}
              className={`px-3 py-1 rounded font-bold transition-all ${selectedQuarters.includes(q) ? 'bg-indigo-600 text-white shadow-md' : 'bg-white text-slate-400 border'}`}>T{q}</button>
          ))}
        </div>

        <div className="flex gap-2">
          <input type="file" onChange={handleCSV} className="text-[9px] w-40" accept=".csv" />
          <input type="file" onChange={handleExcel} className="text-[9px] w-40" accept=".xlsx,.xls" />
        </div>

        <button 
          onClick={handleAutoReconcile} 
          disabled={loading}
          className="bg-indigo-600 text-white px-5 py-2 rounded-lg font-bold uppercase shadow hover:bg-indigo-700 transition flex items-center gap-2 disabled:opacity-50 disabled:cursor-not-allowed">
          <Play size={12} fill="currentColor" /> 
          {loading ? 'Processant...' : 'Conciliar Auto'}
        </button>

        <button onClick={() => {
          const blob = new Blob([JSON.stringify({invoices, bankData, matches, invoiceCash: Array.from(invoiceCash), bankExclusions: Array.from(bankExclusions)}, null, 2)], {type: 'application/json'});
          const a = document.createElement('a'); 
          a.href = URL.createObjectURL(blob); 
          a.download = `backup_${selectedYear}.json`; 
          a.click();
        }} className="p-2 bg-slate-800 text-white rounded hover:bg-black flex items-center gap-1 ml-auto transition-colors">
          <HardDriveDownload size={14}/> Backup
        </button>
      </div>

      {/* BARRA SELECCIÓ MANUAL */}
      {(selectedInvIndices.size > 0 || selectedBankIdx !== null) && (
        <div className="w-full bg-amber-50 border-2 border-amber-300 p-4 rounded-2xl mb-4 flex justify-between items-center shadow-xl no-print">
          <div className="flex gap-10 items-center">
            <div className="flex flex-col border-r border-amber-200 pr-10">
              <span className="text-[9px] font-black text-amber-800 uppercase tracking-widest">Factures ({selectedInvIndices.size})</span>
              <span className="text-xl font-black text-amber-900">{totalSelected.toFixed(2)}€</span>
            </div>
            <span className="text-3xl font-black text-amber-400">➔</span>
            <div className="flex flex-col border-r border-amber-200 pr-10">
              <span className="text-[9px] font-black text-amber-800 uppercase tracking-widest">Banc</span>
              <span className="text-xl font-black text-blue-800">{selectedBankIdx !== null ? `${parseAmount(bankData[selectedBankIdx][COL_BANK_IMPORT]).toFixed(2)}€` : 'Tria un moviment...'}</span>
            </div>
            {selectedBankIdx !== null && selectedInvIndices.size > 0 && (
               <div className={`px-6 py-2 rounded-xl border-4 font-black text-xl shadow-inner bg-white ${Math.abs(totalSelected - parseAmount(bankData[selectedBankIdx][COL_BANK_IMPORT])) <= TOLERANCIA ? "text-emerald-600 border-emerald-300" : "text-rose-600 border-rose-300"}`}>
                Diferència: {(totalSelected - parseAmount(bankData[selectedBankIdx][COL_BANK_IMPORT])).toFixed(2)}€
              </div>
            )}
          </div>
          <div className="flex gap-3">
            <button onClick={async () => {
              setLoading(true);
              try {
                const nm = {...matches}; 
                const conciliacionsPerGuardar = [];
                
                for (const invIdx of selectedInvIndices) {
                  nm[invIdx] = selectedBankIdx;
                  
                  const invHash = getRecordHash(invoices[invIdx], 'factura');
                  const bankHash = getRecordHash(bankData[selectedBankIdx], 'banc');
                  
                  conciliacionsPerGuardar.push({
                    factura_hash: invHash,
                    banc_hash: bankHash,
                    tipus_conciliacio: 'banc'
                  });
                }
                
                // Guardar en batch
                await saveConciliacionsEnMasa(conciliacionsPerGuardar);
                
                setMatches(nm); 
                setSelectedInvIndices(new Set()); 
                setSelectedBankIdx(null);
              } finally {
                setLoading(false);
              }
            }} disabled={selectedInvIndices.size === 0 || selectedBankIdx === null || loading} className="bg-emerald-600 text-white px-10 py-3 rounded-2xl font-black uppercase shadow-lg hover:bg-emerald-700 transition disabled:opacity-50">
              Vincular
            </button>
            <button onClick={() => {setSelectedInvIndices(new Set()); setSelectedBankIdx(null)}} className="bg-white text-amber-800 font-bold px-5 py-3 rounded-2xl border-2 border-amber-200 shadow-sm">
              <RotateCcw size={20}/>
            </button>
          </div>
        </div>
      )}

      {/* TAULES GRID */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-4">
        {/* FACTURES */}
        <div id="win-factures" style={{ resize: 'vertical', height: '600px' }} className="bg-white rounded-xl shadow border flex flex-col overflow-auto min-h-[300px]">
          <div className="p-3 bg-slate-800 text-white flex justify-between items-center no-print sticky top-0 z-20">
             <div className="flex items-center gap-4">
               <span className="font-bold uppercase text-[10px] tracking-widest">Factures</span>
               <div className="flex gap-3 text-[9px] bg-slate-700 px-3 py-1 rounded-lg">
                 <span className="flex items-center gap-1">
                   <span className="text-slate-400">Total:</span>
                   <span className="font-black text-white">{invoiceStats.totals}</span>
                 </span>
                 <span className="w-px bg-slate-600"></span>
                 <span className="flex items-center gap-1">
                   <span className="text-emerald-400">Conciliades:</span>
                   <span className="font-black text-emerald-300">{invoiceStats.conciliades}</span>
                 </span>
                 <span className="w-px bg-slate-600"></span>
                 <span className="flex items-center gap-1">
                   <span className="text-amber-400">Pendents:</span>
                   <span className="font-black text-amber-300">{invoiceStats.pendents}</span>
                 </span>
               </div>
             </div>
             <button onClick={() => printSection('win-factures', 'Llistat de Factures')} className="p-1 hover:bg-slate-700 rounded transition">
               <Printer size={16} />
             </button>
          </div>
          <div className="flex-1">
            <table className="w-full text-left">
              <thead className="bg-gray-50 sticky top-10 border-b z-10 font-bold uppercase text-gray-400 text-[9px]">
                <tr>
                  <th className="p-2">Data</th>
                  <th className="p-2">Últims 4 Núm.</th>
                  <th className="p-2">Proveïdor</th>
                  <th className="p-2 text-right">Import</th>
                  <th className="p-2 text-center">OK</th>
                  <th className="p-2 text-center no-print w-12">Cash</th>
                </tr>
              </thead>
              <tbody>
                {filteredInvoices.map((inv, i) => {
                  const rIdx = invoices.indexOf(inv);
                  const isMatched = matches[rIdx] !== undefined;
                  const isCash = invoiceCash.has(rIdx);
                  const isOK = isMatched || isCash;
                  return (
                    <tr key={i} onClick={() => !isOK && setSelectedInvIndices(prev => {
                      const n=new Set(prev); 
                      n.has(rIdx)?n.delete(rIdx):n.add(rIdx); 
                      return n;
                    })} 
                        className={`border-b cursor-pointer transition-all ${isOK ? 'bg-emerald-50 text-emerald-700 opacity-60' : selectedInvIndices.has(rIdx) ? 'bg-amber-100 ring-2 ring-inset ring-amber-300' : 'hover:bg-gray-50'}`}>
                      <td className="p-2 text-gray-400 font-mono">{inv[COL_FAC_DATA]}</td>
                      <td className="p-2 font-bold">{inv[COL_FAC_NUM] || '-'}</td>
                      <td className="p-2 font-bold">{inv[COL_FAC_PROV]}</td>
                      <td className="p-2 text-right font-black text-indigo-700">{inv[COL_FAC_TOTAL]}€</td>
                      <td className="p-2 text-center">
                        {isOK ? <CheckCircle size={16} className="text-emerald-500 mx-auto"/> : <AlertCircle size={16} className="text-gray-300 mx-auto"/>}
                      </td>
                      <td className="p-2 text-center no-print">
                        {!isMatched && (
                          <button onClick={async (e) => { 
                            e.stopPropagation(); 
                            const n = new Set(invoiceCash);
                            const adding = !n.has(rIdx);
                            
                            const invHash = getRecordHash(inv, 'factura');
                            
                            if (adding) {
                              n.add(rIdx);
                              await saveConciliacio(invHash, null, 'cash');
                            } else {
                              n.delete(rIdx);
                              await deleteConciliacio(invHash);
                            }
                            
                            setInvoiceCash(n); 
                          }} 
                          className={`p-1.5 rounded-full transition-colors ${isCash?'bg-emerald-200 text-emerald-700 border-2 border-emerald-400':'text-gray-300 hover:text-emerald-500'}`}>
                            <Banknote size={16}/>
                          </button>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>

        {/* BANC */}
        <div id="win-banc" style={{ resize: 'vertical', height: '600px' }} className="bg-white rounded-xl shadow border flex flex-col overflow-auto min-h-[300px]">
          <div className="p-3 bg-indigo-900 text-white flex justify-between items-center no-print text-[10px] sticky top-0 z-20">
            <div className="flex items-center gap-4">
              <span className="font-bold uppercase tracking-widest">Extracte Bancari</span>
              <div className="flex gap-3 text-[9px] bg-indigo-800 px-3 py-1 rounded-lg">
                <span className="flex items-center gap-1">
                  <span className="text-indigo-300">Total:</span>
                  <span className="font-black text-white">{bankStats.totals}</span>
                </span>
                <span className="w-px bg-indigo-700"></span>
                <span className="flex items-center gap-1">
                  <span className="text-emerald-400">Conciliades:</span>
                  <span className="font-black text-emerald-300">{bankStats.conciliades}</span>
                </span>
                <span className="w-px bg-indigo-700"></span>
                <span className="flex items-center gap-1">
                  <span className="text-amber-400">Pendents:</span>
                  <span className="font-black text-amber-300">{bankStats.pendents}</span>
                </span>
              </div>
            </div>
            <button onClick={() => printSection('win-banc', 'Extracte Bancari')} className="p-1 hover:bg-indigo-800 rounded transition">
              <Printer size={16} />
            </button>
          </div>
          <div className="flex-1 text-[9px]">
            <table className="w-full text-left">
              <thead className="bg-gray-50 sticky top-10 border-b z-10 text-gray-400 font-bold uppercase">
                <tr>
                  <th className="p-2">Data</th>
                  <th className="p-2">Moviment</th>
                  <th className="p-2 text-right">Import</th>
                  <th className="p-2 text-center no-print w-12">SF</th>
                </tr>
              </thead>
              <tbody>
                {filteredBank.map((row, i) => {
                  const rIdx = bankData.indexOf(row);
                  const isUsed = Object.values(matches).includes(rIdx);
                  const isEx = bankExclusions.has(rIdx);
                  return (
                    <tr key={i} onClick={() => !isUsed && !isEx && setSelectedBankIdx(rIdx)} className={`border-b cursor-pointer transition-all ${isUsed || isEx ? 'bg-emerald-50 text-emerald-600 opacity-60' : selectedBankIdx === rIdx ? 'bg-amber-100 ring-2 ring-inset ring-amber-300' : 'hover:bg-gray-50'}`}>
                      <td className="p-2 text-gray-400 font-mono">{row[COL_BANK_DATA] || '-'}</td>
                      <td className="p-2 truncate max-w-[250px] italic">{String(row[COL_BANK_DESC])}</td>
                      <td className="p-2 text-right font-black text-rose-700">{row[COL_BANK_IMPORT]}€</td>
                      <td className="p-2 text-center no-print">
                        {!isUsed && (
                          <button onClick={async (e) => {
                            e.stopPropagation(); 
                            const n = new Set(bankExclusions);
                            const adding = !n.has(rIdx);
                            
                            // CORRECCIÓ IMPORTANT: guardar el hash del banc com a factura_hash
                            const bankHash = getRecordHash(row, 'banc');
                            
                            if (adding) {
                              n.add(rIdx);
                              await saveConciliacio(bankHash, null, 'exclos');
                            } else {
                              n.delete(rIdx);
                              await deleteConciliacio(bankHash);
                            }
                            
                            setBankExclusions(n); 
                          }} 
                          className={`p-1.5 rounded-full transition-colors ${isEx?'bg-emerald-200 text-emerald-700 border-2 border-emerald-400':'text-gray-300 hover:text-emerald-500'}`}>
                            <BookmarkCheck size={16}/>
                          </button>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      {/* RESUM CONCILIACIONS */}
      <div id="win-paired" style={{ resize: 'vertical', height: '350px' }} className="w-full bg-white rounded-xl shadow-2xl border-2 border-emerald-300 overflow-auto mb-12 min-h-[150px] flex flex-col">
        <div className="p-3 bg-emerald-600 text-white flex justify-between items-center no-print sticky top-0 z-20">
          <div className="flex items-center gap-4">
            <span className="font-black uppercase text-[10px] tracking-widest text-white">
              Resum de Factures Conciliades
            </span>
            <div className="flex gap-3 text-[9px] bg-emerald-700 px-3 py-1 rounded-lg">
              <span className="flex items-center gap-1">
                <span className="text-emerald-200">Total:</span>
                <span className="font-black text-white">{filteredMatchesList.length + filteredCashList.length + filteredBankExclusionsList.length}</span>
              </span>
              <span className="w-px bg-emerald-600"></span>
              <span className="flex items-center gap-1">
                <span className="text-blue-300">Amb factura:</span>
                <span className="font-black text-white">{filteredMatchesList.length + filteredCashList.length}</span>
              </span>
              <span className="w-px bg-emerald-600"></span>
              <span className="flex items-center gap-1">
                <span className="text-amber-300">Sense factura:</span>
                <span className="font-black text-white">{filteredBankExclusionsList.length}</span>
              </span>
            </div>
          </div>
          <div className="flex gap-4 items-center">
            <button onClick={() => printSection('win-paired', 'Informe de Factures Conciliades')} className="p-1.5 hover:bg-emerald-500 rounded-full text-white bg-emerald-700 shadow-inner transition">
              <Printer size={18} />
            </button>
            <button onClick={() => setShowPaired(!showPaired)} className="p-1 text-white hover:bg-emerald-500 rounded-full">
              <ChevronDown size={24} className={showPaired ? 'rotate-180' : ''}/>
            </button>
          </div>
        </div>
        {showPaired && (
          <div className="flex-1">
            <table className="w-full text-left text-[9px] border-collapse">
              <thead className="bg-emerald-50 sticky top-10 border-b font-bold uppercase text-emerald-800 z-10">
                <tr>
                  <th className="p-2 w-16 text-center">Tipus</th>
                  <th className="p-2 w-20">Data Fac</th>
                  <th className="p-2 w-16">Núm. Fac</th>
                  <th className="p-2">Proveïdor</th>
                  <th className="p-2 text-right border-r">Import Fac.</th>
                  <th className="p-2 w-20">Data Banc</th>
                  <th className="p-2 pl-4">Banc / Alerta</th>
                  <th className="p-2 text-right">Import Banc</th>
                  <th className="p-2 no-print text-center w-14">Acció</th>
                </tr>
              </thead>
              <tbody>
                {/* MOVIMENTS SENSE FACTURA (SF) */}
                {filteredBankExclusionsList.map(bankIdx => (
                  <tr key={`sf-${bankIdx}`} className="border-b bg-orange-50/50 text-orange-900 font-medium">
                    <td className="p-2 font-black text-[9px] text-orange-600 text-center uppercase tracking-tighter">SF</td>
                    <td className="p-2 text-gray-400" colSpan="4">
                      <span className="italic font-bold text-orange-800">Moviment sense factura associada</span>
                    </td>
                    <td className="p-2 font-mono text-gray-400">{bankData[bankIdx][COL_BANK_DATA]}</td>
                    <td className="p-2 pl-4">
                      <span className="italic text-gray-600 truncate max-w-xs block">{String(bankData[bankIdx][COL_BANK_DESC])}</span>
                    </td>
                    <td className="p-2 text-right font-black text-rose-700">{bankData[bankIdx][COL_BANK_IMPORT]}€</td>
                    <td className="p-2 text-center no-print">
                      <button onClick={async () => { 
                        const n = new Set(bankExclusions); 
                        n.delete(bankIdx); 
                        setBankExclusions(n);
                        
                        const bankHash = getRecordHash(bankData[bankIdx], 'banc');
                        await deleteConciliacio(bankHash);
                      }} className="text-rose-500 hover:bg-rose-100 p-1.5 rounded-full transition-colors">
                        <XCircle size={18}/>
                      </button>
                    </td>
                  </tr>
                ))}
                
                {/* PAGAMENTS EN CASH */}
                {filteredCashList.map(idx => (
                  <tr key={`c-${idx}`} className="border-b bg-emerald-50/20 italic text-emerald-900 font-medium">
                    <td className="p-2 font-black text-[9px] text-emerald-600 text-center uppercase tracking-tighter">Cash</td>
                    <td className="p-2 font-mono text-gray-400">{invoices[idx][COL_FAC_DATA]}</td>
                    <td className="p-2 font-bold text-gray-600">{invoices[idx][COL_FAC_NUM] || '-'}</td>
                    <td className="p-2 font-bold">{invoices[idx][COL_FAC_PROV]}</td>
                    <td className="p-2 text-right font-black border-r border-emerald-100">{invoices[idx][COL_FAC_TOTAL]}€</td>
                    <td className="p-2 pl-4 text-gray-400 italic" colSpan="3">Pagament fora de circuit bancari</td>
                    <td className="p-2 text-center no-print">
                      <button onClick={async () => { 
                        const n = new Set(invoiceCash); 
                        n.delete(idx); 
                        setInvoiceCash(n);
                        
                        const invHash = getRecordHash(invoices[idx], 'factura');
                        await deleteConciliacio(invHash);
                      }} className="text-rose-500 hover:bg-rose-100 p-1.5 rounded-full transition-colors">
                        <XCircle size={18}/>
                      </button>
                    </td>
                  </tr>
                ))}
                
                {/* CONCILIACIONS BANC-FACTURA */}
                {filteredMatchesList.map(([invIdx, bIdx]) => {
                  const qInv = getQuarter(invoices[invIdx][COL_FAC_DATA]);
                  const qBank = getQuarter(bankData[bIdx][COL_BANK_DATA]);
                  return (
                    <tr key={invIdx} className="border-b hover:bg-emerald-50 font-medium transition-colors">
                      <td className="p-2 font-black text-blue-800 text-[9px] text-center uppercase tracking-tighter">Banc</td>
                      <td className="p-2 font-mono text-gray-400">{invoices[invIdx][COL_FAC_DATA]}</td>
                      <td className="p-2 font-bold text-indigo-600">{invoices[invIdx][COL_FAC_NUM] || '-'}</td>
                      <td className="p-2 font-bold">{invoices[invIdx][COL_FAC_PROV]}</td>
                      <td className="p-2 text-right font-black border-r">{invoices[invIdx][COL_FAC_TOTAL]}€</td>
                      <td className="p-2 font-mono text-gray-400">
                        {qInv !== qBank && (
                          <span className="inline-flex items-center gap-1 bg-rose-600 text-white px-1 py-0.5 rounded text-[8px] font-black mr-1">
                            <AlertCircle size={8}/> T{qBank}
                          </span>
                        )}
                        {bankData[bIdx][COL_BANK_DATA]}
                      </td>
                      <td className="p-2 pl-4">
                        <span className="italic text-gray-500 truncate max-w-xs block">{String(bankData[bIdx][COL_BANK_DESC])}</span>
                      </td>
                      <td className="p-2 text-right font-black text-rose-700">{bankData[bIdx][COL_BANK_IMPORT]}€</td>
                      <td className="p-2 text-center no-print">
                        <button onClick={async () => { 
                          const n={...matches}; 
                          delete n[invIdx]; 
                          setMatches(n);
                          
                          const invHash = getRecordHash(invoices[invIdx], 'factura');
                          await deleteConciliacio(invHash);
                        }} className="text-rose-500 hover:bg-rose-100 p-1.5 rounded-full transition-colors">
                          <Unlink size={18}/>
                        </button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
