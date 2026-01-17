// ============================================================================
// 🎯 VERSIÓ v35 BIGGER FONTS - 17 Gener 2026
// ============================================================================
// - ✅ UI: Augment general de la mida del text (de 9px a 12px/13px)
// - ✅ UI: Els números (imports) es veuen més grans per facilitar la lectura
// - ✅ Manté tota la funcionalitat (Filtres, Cerca, Sticky, Delete, etc.)
// ============================================================================

import React, { useState, useMemo, useEffect } from 'react';
import { 
  CheckCircle, AlertCircle, Printer, ChevronDown, Play, XCircle, 
  Unlink, BookmarkCheck, Banknote, RotateCcw, Calendar, HardDriveDownload, 
  Trash2, Search, Filter 
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
  console.log('🎯 ReconciliationTool v35 BIGGER FONTS');
  
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
  
  const [invoiceSearch, setInvoiceSearch] = useState('');
  const [bankSearch, setBankSearch] = useState('');
  const [showPendingInv, setShowPendingInv] = useState(false);
  const [showPendingBank, setShowPendingBank] = useState(false);

  const [loading, setLoading] = useState(false);
  const [loadingStatus, setLoadingStatus] = useState('');

  const COL_FAC_DATA = "DATA";
  const COL_FAC_NUM = "ULTIMA 4 DIGITS NUMERO FACTURA";
  const COL_FAC_PROV = "PROVEEDOR";
  const COL_FAC_TOTAL = "TOTAL FACTURA";
  
  const COL_BANK_DATA = "F. Operativa";
  const COL_BANK_DESC = "Concepto";
  const COL_BANK_IMPORT = "Importe";

  // --- HELPERS ---
  const safeGet = (obj, key) => {
    if (!obj || !key) return "";
    if (obj[key] !== undefined) return obj[key];
    const cleanKey = key.toLowerCase().trim();
    const foundKey = Object.keys(obj).find(k => k.toLowerCase().trim() === cleanKey);
    return foundKey ? obj[foundKey] : "";
  };

  const getQuarter = (d) => {
    if (!d || typeof d !== 'string') return -1;
    const cleanD = d.trim().replace(/-/g, '/').replace(/\./g, '/');
    const parts = cleanD.split('/');
    if (parts.length < 2) return -1;
    return Math.ceil(parseInt(parts[1]) / 3) || -1;
  };

  const getYear = (d) => {
    if (!d || typeof d !== 'string') return -1;
    const cleanD = d.trim().replace(/-/g, '/').replace(/\./g, '/');
    const parts = cleanD.split('/');
    if (parts.length < 3) return -1;
    let y = parseInt(parts[2]);
    return y < 100 ? 2000 + y : y;
  };

  const parseAmount = (v) => {
    if (!v) return 0;
    let s = String(v).trim();
    if (s.endsWith('-')) s = '-' + s.slice(0, -1);
    const isNegative = s.includes('-');
    let clean = s.replace(/[^\d,.]/g, '');
    if (clean.includes(',') && clean.includes('.')) clean = clean.replace(/\./g, '').replace(',', '.');
    else if (clean.includes(',')) clean = clean.replace(',', '.');
    const floatVal = parseFloat(clean);
    if (isNaN(floatVal)) return 0;
    return isNegative ? -Math.abs(floatVal) : Math.abs(floatVal);
  };

  const getBaseHash = (item, tipus) => {
    try {
      const dateStr = tipus === 'factura' ? safeGet(item, COL_FAC_DATA) : safeGet(item, COL_BANK_DATA);
      const amount = tipus === 'factura' ? safeGet(item, COL_FAC_TOTAL) : safeGet(item, COL_BANK_IMPORT);
      const desc = tipus === 'factura' ? safeGet(item, COL_FAC_PROV) : safeGet(item, COL_BANK_DESC);
      const str = `${tipus}|${dateStr}|${amount}|${desc}`.substring(0, 200);
      return btoa(encodeURIComponent(str)).substring(0, 100);
    } catch (e) {
      return Math.random().toString(36);
    }
  };

  // --- LECTURA I ESCRIPTURA ---
  const fetchAllRows = async (tableName) => {
    let allData = [];
    let page = 0;
    const pageSize = 1000;
    let hasMore = true;
    while (hasMore) {
      const from = page * pageSize;
      const to = (page + 1) * pageSize - 1;
      setLoadingStatus(`Baixant ${tableName}... (${allData.length} files)`);
      const { data, error } = await supabase.from(tableName).select('*').range(from, to);
      if (error) throw error;
      if (data && data.length > 0) {
        allData = [...allData, ...data];
        if (data.length < pageSize) hasMore = false; else page++;
      } else { hasMore = false; }
    }
    return allData;
  };

  const syncSupabaseBatch = async (tipus, items, existingHashes) => {
    setLoadingStatus(`Analitzant ${items.length} registres...`);
    const finalPayloads = [];
    const localBatchHashes = new Set(); 
    let duplicatsReals = 0;
    let anyInvalid = 0;
    const batchId = Date.now();

    items.forEach((item, index) => {
      const dateStr = tipus === 'factura' ? safeGet(item, COL_FAC_DATA) : safeGet(item, COL_BANK_DATA);
      const year = getYear(dateStr);
      const quarter = getQuarter(dateStr);
      if (year > 2000) {
         let hash = getBaseHash(item, tipus);
         const originalHash = hash;
         let counter = 0;
         while (existingHashes.has(hash) || localBatchHashes.has(hash)) {
             counter++;
             hash = `${originalHash}_${counter}`;
         }
         if (counter > 0) duplicatsReals++;
         localBatchHashes.add(hash);
         const itemWithMeta = { ...item, _batch_id: batchId, _item_index: index };
         finalPayloads.push({ tipus, contingut: itemWithMeta, ejercicio: year, trimestre: quarter, unique_hash: hash });
      } else { anyInvalid++; }
    });

    const BATCH_SIZE = 100;
    let successCount = 0;
    let errorCount = 0;
    for (let i = 0; i < finalPayloads.length; i += BATCH_SIZE) {
        setLoadingStatus(`Pujant bloc ${Math.ceil(i/BATCH_SIZE) + 1}/${Math.ceil(finalPayloads.length/BATCH_SIZE)}...`);
        const batch = finalPayloads.slice(i, i + BATCH_SIZE);
        const { error } = await supabase.from('registres_comptables').upsert(batch, { onConflict: 'unique_hash' });
        if (error) { errorCount += batch.length; } else { successCount += batch.length; }
        await new Promise(resolve => setTimeout(resolve, 50)); 
    }
    return { totalLlegit: items.length, guardats: successCount, duplicatsRenombrats: duplicatsReals, errors: errorCount, anyInvalid: anyInvalid };
  };

  const deleteSelectedInvoices = async () => {
    if (selectedInvIndices.size === 0) return;
    if (!window.confirm(`⚠️ Esborrar ${selectedInvIndices.size} factures?`)) return;
    setLoading(true);
    try {
        const hashesToDelete = [];
        selectedInvIndices.forEach(idx => {
            if (invoices[idx] && invoices[idx].unique_hash) hashesToDelete.push(invoices[idx].unique_hash);
        });
        if (hashesToDelete.length > 0) {
            await supabase.from('registres_comptables').delete().in('unique_hash', hashesToDelete);
            await supabase.from('conciliacions').delete().in('factura_hash', hashesToDelete);
            window.location.reload();
        }
    } catch (error) { alert('❌ Error: ' + error.message); setLoading(false); }
  };

  const saveConciliacionsEnMasa = async (conciliacions) => {
    if (conciliacions.length === 0) return true;
    try {
      const uniqueMap = new Map();
      conciliacions.forEach(c => { if (c.factura_hash && c.tipus_conciliacio) uniqueMap.set(c.factura_hash, c); });
      const validConciliacions = Array.from(uniqueMap.values());
      const { error } = await supabase.from('conciliacions').upsert(validConciliacions, { onConflict: 'factura_hash' });
      if (error) throw error;
      return true;
    } catch (error) { return false; }
  };

  const saveConciliacio = async (facturaHash, bancHash = null, tipus = 'banc') => {
    try {
      const { error } = await supabase.from('conciliacions').upsert(
        [{ factura_hash: facturaHash, banc_hash: bancHash, tipus_conciliacio: tipus }],
        { onConflict: 'factura_hash' }
      );
      if (error) throw error;
      return true;
    } catch (error) { return false; }
  };

  const deleteConciliacio = async (facturaHash) => {
    try {
      const { error } = await supabase.from('conciliacions').delete().eq('factura_hash', facturaHash);
      if (error) throw error;
      return true;
    } catch (error) { return false; }
  };

  // --- CARREGAR DADES ---
  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      try {
        const registres = await fetchAllRows('registres_comptables');
        const conciliacions = await fetchAllRows('conciliacions');
        setLoadingStatus('Processant dades...');
        if (registres) {
          const facturesData = registres.filter(d => d.tipus === 'factura');
          const bancData = registres.filter(d => d.tipus === 'banc');
          
          const invoicesList = facturesData.map(d => ({ ...d.contingut, unique_hash: d.unique_hash }));
          const bankList = bancData.map(d => ({ ...d.contingut, unique_hash: d.unique_hash }));
          
          // Smart Sort: Lots nous primer, ordre original dins del lot
          const smartSort = (a, b) => {
             const batchDiff = (b._batch_id || 0) - (a._batch_id || 0);
             if (batchDiff !== 0) return batchDiff;
             return (a._item_index || 0) - (b._item_index || 0);
          };

          invoicesList.sort(smartSort);
          bankList.sort(smartSort);
          
          setInvoices(invoicesList);
          setBankData(bankList);
          
          const invHashToIdx = new Map();
          invoicesList.forEach((inv, idx) => invHashToIdx.set(inv.unique_hash, idx));
          
          const bankHashToIdx = new Map();
          bankList.forEach((bank, idx) => bankHashToIdx.set(bank.unique_hash, idx));
          
          const newMatches = {};
          const newCash = new Set();
          const newExclusions = new Set();
          
          if (conciliacions) {
            conciliacions.forEach(conc => {
              if (conc.tipus_conciliacio === 'banc' && conc.banc_hash) {
                const invIdx = invHashToIdx.get(conc.factura_hash);
                const bankIdx = bankHashToIdx.get(conc.banc_hash);
                if (invIdx !== undefined && bankIdx !== undefined) newMatches[invIdx] = bankIdx;
              } else if (conc.tipus_conciliacio === 'cash') {
                const invIdx = invHashToIdx.get(conc.factura_hash);
                if (invIdx !== undefined) newCash.add(invIdx);
              } else if (conc.tipus_conciliacio === 'exclos') {
                const bankIdx = bankHashToIdx.get(conc.factura_hash);
                if (bankIdx !== undefined) newExclusions.add(bankIdx);
              }
            });
          }
          setMatches(newMatches);
          setInvoiceCash(newCash);
          setBankExclusions(newExclusions);
          
          const years = [...facturesData, ...bancData].map(d => d.ejercicio).filter(y => y > 2000);
          if (years.length > 0) setSelectedYear(Math.max(...years));
        }
      } catch (error) { alert('Error carregant dades: ' + error.message); } 
      finally { setLoading(false); setLoadingStatus(''); }
    };
    fetchData();
  }, []);

  // --- FILTRES ---
  const filteredInvoices = useMemo(() => {
    return invoices.filter(inv => {
      const d = safeGet(inv, COL_FAC_DATA);
      const matchesBase = getYear(d) === selectedYear && selectedQuarters.includes(getQuarter(d));
      if (!matchesBase) return false;

      if (invoiceSearch.trim() !== '') {
        const searchLower = invoiceSearch.toLowerCase();
        const textMatch = safeGet(inv, COL_FAC_PROV).toLowerCase().includes(searchLower) || 
                          safeGet(inv, COL_FAC_TOTAL).toString().toLowerCase().includes(searchLower);
        if (!textMatch) return false;
      }

      if (showPendingInv) {
        const rIdx = invoices.indexOf(inv);
        const isResolved = matches[rIdx] !== undefined || invoiceCash.has(rIdx);
        if (isResolved) return false;
      }
      return true;
    });
  }, [invoices, selectedQuarters, selectedYear, invoiceSearch, showPendingInv, matches, invoiceCash]);

  const filteredBank = useMemo(() => {
    return bankData.filter(row => {
      const d = safeGet(row, COL_BANK_DATA);
      const matchesBase = getYear(d) === selectedYear && selectedQuarters.includes(getQuarter(d));
      if (!matchesBase) return false;

      if (bankSearch.trim() !== '') {
        const searchLower = bankSearch.toLowerCase();
        const textMatch = safeGet(row, COL_BANK_DESC).toLowerCase().includes(searchLower) || 
                          safeGet(row, COL_BANK_IMPORT).toString().toLowerCase().includes(searchLower);
        if (!textMatch) return false;
      }

      if (showPendingBank) {
        const rIdx = bankData.indexOf(row);
        const isResolved = Object.values(matches).includes(rIdx) || bankExclusions.has(rIdx);
        if (isResolved) return false;
      }
      return true;
    });
  }, [bankData, selectedQuarters, selectedYear, bankSearch, showPendingBank, matches, bankExclusions]);

  const filteredMatchesList = useMemo(() => Object.entries(matches).filter(([i]) => filteredInvoices.includes(invoices[i])), [matches, invoices, filteredInvoices]);
  const filteredCashList = useMemo(() => [...invoiceCash].filter(i => filteredInvoices.includes(invoices[i])), [invoiceCash, invoices, filteredInvoices]);
  const filteredBankExclusionsList = useMemo(() => [...bankExclusions].filter(i => filteredBank.includes(bankData[i])), [bankExclusions, bankData, filteredBank]);

  const invoiceStats = useMemo(() => {
    const conciliades = filteredInvoices.filter((inv, i) => {
      const rIdx = invoices.indexOf(inv);
      return matches[rIdx] !== undefined || invoiceCash.has(rIdx);
    }).length;
    return { totals: filteredInvoices.length, conciliades: conciliades, pendents: filteredInvoices.length - conciliades };
  }, [filteredInvoices, invoices, matches, invoiceCash]);

  const bankStats = useMemo(() => {
    const conciliades = filteredBank.filter((row, i) => {
      const rIdx = bankData.indexOf(row);
      return Object.values(matches).includes(rIdx) || bankExclusions.has(rIdx);
    }).length;
    return { totals: filteredBank.length, conciliades: conciliades, pendents: filteredBank.length - conciliades };
  }, [filteredBank, bankData, matches, bankExclusions]);

  const handleAutoReconcile = async () => {
    setLoading(true);
    setLoadingStatus('Cercant coincidències...');
    try {
      const newMatches = { ...matches };
      const usedBank = new Set([...Object.values(matches), ...bankExclusions]);
      const conciliacionsPerGuardar = [];
      let trobadesNoves = 0;

      for (const inv of filteredInvoices) {
        const rIdx = invoices.indexOf(inv);
        if (newMatches[rIdx] || invoiceCash.has(rIdx)) continue;
        
        const invAmt = parseAmount(safeGet(inv, COL_FAC_TOTAL));
        const bIdx = bankData.findIndex((b, j) => {
          if (usedBank.has(j)) return false;
          const bankAmt = parseAmount(safeGet(b, COL_BANK_IMPORT));
          return Math.abs(invAmt + bankAmt) <= TOLERANCIA;
        });

        if (bIdx !== -1) {
          newMatches[rIdx] = bIdx;
          usedBank.add(bIdx);
          trobadesNoves++;
          conciliacionsPerGuardar.push({
            factura_hash: inv.unique_hash,
            banc_hash: bankData[bIdx].unique_hash,
            tipus_conciliacio: 'banc'
          });
        }
      }
      if (conciliacionsPerGuardar.length > 0) {
        setLoadingStatus('Guardant resultats...');
        await saveConciliacionsEnMasa(conciliacionsPerGuardar);
      }
      setMatches(newMatches);
      alert(`✅ AUTO-MATCH: ${trobadesNoves} noves parelles.`);
    } catch (error) { console.error(error); } finally { setLoading(false); }
  };

  const handleCSV = (e) => {
    setLoading(true);
    Papa.parse(e.target.files[0], {
      header: true, delimiter: "", skipEmptyLines: true, transformHeader: h => h.trim(),
      complete: async (r) => {
        try {
          const existingHashes = new Set(invoices.map(i => i.unique_hash));
          await syncSupabaseBatch('factura', r.data, existingHashes);
          window.location.reload(); 
        } catch (error) { alert('❌ Error processant CSV.'); setLoading(false); }
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
        const hIdx = rows.findIndex(r => {
          const rowStr = r.map(c => String(c).trim().toLowerCase()).join('|');
          return rowStr.includes('operativa') || rowStr.includes('concepto') || rowStr.includes('importe');
        });
        if (hIdx !== -1) {
          const h = rows[hIdx].map(x => String(x).trim());
          const news = rows.slice(hIdx + 1)
            .filter(r => /^\d{1,2}[\/.-]\d{1,2}[\/.-]\d{2,4}$/.test(String(r[0] || "").trim()))
            .map(r => {
              const obj = {}; h.forEach((col, i) => { if (col) obj[col] = String(r[i] || "").trim(); });
              return obj;
            });
          const existingHashes = new Set(bankData.map(b => b.unique_hash));
          await syncSupabaseBatch('banc', news, existingHashes);
          alert(`✅ Dades carregades. Es recarregarà l'aplicació.`);
          window.location.reload();
        } else { alert('❌ No s\'ha trobat la fila de capçaleres.'); setLoading(false); }
      } catch (error) { alert('❌ Error Excel: ' + error.message); setLoading(false); }
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
    selectedInvIndices.forEach(idx => sum += parseAmount(safeGet(invoices[idx], COL_FAC_TOTAL)));
    return sum;
  }, [selectedInvIndices, invoices]);

  const bankSelectedAmt = selectedBankIdx !== null ? parseAmount(safeGet(bankData[selectedBankIdx], COL_BANK_IMPORT)) : 0;
  const diff = Math.abs(totalSelected + bankSelectedAmt);

  return (
    <div className="w-full min-h-screen bg-slate-100 p-2 font-sans text-xs">
      {loading && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-[100]">
          <div className="bg-white p-8 rounded-2xl shadow-2xl flex flex-col items-center gap-4">
            <div className="animate-spin rounded-full h-16 w-16 border-b-4 border-indigo-600"></div>
            <div className="text-center">
               <p className="text-lg font-bold text-gray-700">Carregant dades...</p>
               <p className="text-sm text-gray-500 mt-2 font-mono">{loadingStatus}</p>
            </div>
          </div>
        </div>
      )}
      
      <iframe id="ifmcontentstoprint" className="hidden" title="print"></iframe>

      {/* HEADER */}
      <div className="sticky top-0 z-50 w-full bg-white p-4 rounded-b-xl shadow-md mb-4 border-b flex items-center gap-4 no-print flex-wrap">
        <h1 className="text-xl font-black text-indigo-700 italic border-r pr-4 uppercase tracking-tighter">FinMatch v35</h1>
        
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

        <button onClick={handleAutoReconcile} disabled={loading} className="bg-indigo-600 text-white px-5 py-2 rounded-lg font-bold uppercase shadow hover:bg-indigo-700 transition flex items-center gap-2 disabled:opacity-50">
          <Play size={12} fill="currentColor" /> {loading ? '...' : 'Auto-Conciliar'}
        </button>

        <button onClick={() => {
          const blob = new Blob([JSON.stringify({invoices, bankData, matches, invoiceCash: Array.from(invoiceCash), bankExclusions: Array.from(bankExclusions)}, null, 2)], {type: 'application/json'});
          const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = `backup_${selectedYear}.json`; a.click();
        }} className="p-2 bg-slate-800 text-white rounded hover:bg-black flex items-center gap-1 ml-auto transition-colors">
          <HardDriveDownload size={14}/> Backup
        </button>
      </div>

      {/* MANUAL BAR (STICKY) */}
      {(selectedInvIndices.size > 0 || selectedBankIdx !== null) && (
        <div className="sticky top-[80px] z-40 w-full bg-amber-50 border-2 border-amber-300 p-4 rounded-2xl mb-4 flex justify-between items-center shadow-xl no-print">
          <div className="flex gap-10 items-center">
            <div className="flex flex-col border-r border-amber-200 pr-10">
              <span className="text-[9px] font-black text-amber-800 uppercase tracking-widest">Factures ({selectedInvIndices.size})</span>
              <span className={`text-xl font-black ${totalSelected < 0 ? 'text-rose-600' : 'text-amber-900'}`}>{totalSelected.toFixed(2)}€</span>
            </div>
            <span className="text-3xl font-black text-amber-400">➔</span>
            <div className="flex flex-col border-r border-amber-200 pr-10">
              <span className="text-[9px] font-black text-amber-800 uppercase tracking-widest">Banc</span>
              <span className={`text-xl font-black ${bankSelectedAmt < 0 ? 'text-rose-600' : 'text-emerald-700'}`}>{selectedBankIdx !== null ? `${bankSelectedAmt.toFixed(2)}€` : 'Tria...'}</span>
            </div>
            {selectedBankIdx !== null && selectedInvIndices.size > 0 && (
               <div className={`px-6 py-2 rounded-xl border-4 font-black text-xl shadow-inner bg-white ${diff <= TOLERANCIA ? "text-emerald-600 border-emerald-300" : "text-rose-600 border-rose-300"}`}>
                Dif: {diff.toFixed(2)}€
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
                  conciliacionsPerGuardar.push({
                    factura_hash: invoices[invIdx].unique_hash,
                    banc_hash: bankData[selectedBankIdx].unique_hash,
                    tipus_conciliacio: 'banc'
                  });
                }
                await saveConciliacionsEnMasa(conciliacionsPerGuardar);
                setMatches(nm); setSelectedInvIndices(new Set()); setSelectedBankIdx(null);
              } finally { setLoading(false); }
            }} disabled={selectedInvIndices.size === 0 || selectedBankIdx === null || loading} className="bg-emerald-600 text-white px-10 py-3 rounded-2xl font-black uppercase shadow-lg hover:bg-emerald-700 transition disabled:opacity-50">
              Vincular
            </button>
            
            {selectedInvIndices.size > 0 && (
               <button onClick={deleteSelectedInvoices} disabled={loading} className="bg-rose-600 text-white px-4 py-3 rounded-2xl font-bold uppercase shadow-lg hover:bg-rose-700 transition disabled:opacity-50 flex items-center gap-2">
                 <Trash2 size={18}/> Esborrar
               </button>
            )}

            <button onClick={() => {setSelectedInvIndices(new Set()); setSelectedBankIdx(null)}} className="bg-white text-amber-800 font-bold px-5 py-3 rounded-2xl border-2 border-amber-200 shadow-sm"><RotateCcw size={20}/></button>
          </div>
        </div>
      )}

      {/* TAULES GRID */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-4">
        {/* FACTURES */}
        <div id="win-factures" style={{ resize: 'vertical', height: '600px' }} className="bg-white rounded-xl shadow border flex flex-col overflow-auto min-h-[300px]">
          <div className="p-3 bg-slate-800 text-white flex justify-between items-center no-print sticky top-0 z-20">
             <div className="flex items-center gap-4">
               <span className="font-bold uppercase text-[10px] tracking-widest">Factures ({invoiceStats.totals})</span>
               <div className="relative">
                 <Search size={14} className="absolute left-2 top-1.5 text-slate-400"/>
                 <input type="text" placeholder="Cercar..." value={invoiceSearch} onChange={e => setInvoiceSearch(e.target.value)}
                   className="pl-8 pr-2 py-1 text-xs rounded bg-slate-700 border-none text-white placeholder-slate-400 outline-none focus:ring-1 focus:ring-slate-500 w-32"/>
               </div>
               
               <button 
                 onClick={() => setShowPendingInv(!showPendingInv)}
                 className={`p-1.5 rounded transition-colors ${showPendingInv ? 'bg-amber-400 text-slate-900' : 'bg-slate-700 text-slate-400 hover:text-white'}`}
                 title="Només Pendents"
               >
                 <Filter size={16}/>
               </button>

               <div className="flex gap-3 text-[9px] bg-slate-700 px-3 py-1 rounded-lg">
                 <span className="text-emerald-400">OK: {invoiceStats.conciliades}</span>
                 <span className="w-px bg-slate-600"></span>
                 <span className="text-amber-400">Pendent: {invoiceStats.pendents}</span>
               </div>
             </div>
             <button onClick={() => printSection('win-factures', 'Llistat de Factures')} className="p-1 hover:bg-slate-700 rounded transition"><Printer size={16} /></button>
          </div>
          <div className="flex-1">
            <table className="w-full text-left">
              <thead className="bg-gray-50 sticky top-0 border-b z-10 font-bold uppercase text-gray-400 text-[11px]">
                <tr>
                  <th className="p-2">Data</th>
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
                  const amt = parseAmount(safeGet(inv, COL_FAC_TOTAL));
                  return (
                    <tr key={i} onClick={() => !isOK && setSelectedInvIndices(prev => { const n=new Set(prev); n.has(rIdx)?n.delete(rIdx):n.add(rIdx); return n; })} 
                        className={`border-b cursor-pointer transition-all ${isOK ? 'bg-emerald-50 text-emerald-700 opacity-60' : selectedInvIndices.has(rIdx) ? 'bg-amber-100 ring-2 ring-inset ring-amber-300' : 'hover:bg-gray-50'}`}>
                      <td className="p-2 text-gray-400 font-mono">{safeGet(inv, COL_FAC_DATA)}</td>
                      <td className="p-2 font-bold">{safeGet(inv, COL_FAC_PROV)}</td>
                      <td className={`p-2 text-right font-black text-[13px] ${amt < 0 ? 'text-rose-600' : 'text-indigo-700'}`}>{safeGet(inv, COL_FAC_TOTAL)}€</td>
                      <td className="p-2 text-center">{isOK ? <CheckCircle size={16} className="text-emerald-500 mx-auto"/> : <AlertCircle size={16} className="text-gray-300 mx-auto"/>}</td>
                      <td className="p-2 text-center no-print">
                        {!isMatched && (
                          <button onClick={async (e) => { 
                            e.stopPropagation(); const n = new Set(invoiceCash); const adding = !n.has(rIdx);
                            const invHash = inv.unique_hash;
                            if (adding) { n.add(rIdx); await saveConciliacio(invHash, null, 'cash'); } 
                            else { n.delete(rIdx); await deleteConciliacio(invHash); }
                            setInvoiceCash(n);
                          }} className={`p-1.5 rounded-full transition-colors ${isCash?'bg-emerald-200 text-emerald-700 border-2 border-emerald-400':'text-gray-300 hover:text-emerald-500'}`}><Banknote size={16}/></button>
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
              <span className="font-bold uppercase tracking-widest">Banc ({bankStats.totals})</span>
              <div className="relative">
                 <Search size={14} className="absolute left-2 top-1.5 text-indigo-300"/>
                 <input type="text" placeholder="Cercar..." value={bankSearch} onChange={e => setBankSearch(e.target.value)}
                   className="pl-8 pr-2 py-1 text-xs rounded bg-indigo-800 border-none text-white placeholder-indigo-300 outline-none focus:ring-1 focus:ring-indigo-500 w-32"/>
               </div>

               <button 
                 onClick={() => setShowPendingBank(!showPendingBank)}
                 className={`p-1.5 rounded transition-colors ${showPendingBank ? 'bg-amber-400 text-indigo-900' : 'bg-indigo-800 text-indigo-300 hover:text-white'}`}
                 title="Només Pendents"
               >
                 <Filter size={16}/>
               </button>

              <div className="flex gap-3 text-[9px] bg-indigo-800 px-3 py-1 rounded-lg">
                <span className="text-emerald-400">OK: {bankStats.conciliades}</span>
                <span className="w-px bg-indigo-700"></span>
                <span className="text-amber-400">Pendent: {bankStats.pendents}</span>
              </div>
            </div>
            <button onClick={() => printSection('win-banc', 'Extracte Bancari')} className="p-1 hover:bg-indigo-800 rounded transition"><Printer size={16} /></button>
          </div>
          <div className="flex-1">
            <table className="w-full text-left">
              <thead className="bg-gray-50 sticky top-0 border-b z-10 font-bold uppercase text-gray-400 text-[11px]">
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
                  const amt = parseAmount(safeGet(row, COL_BANK_IMPORT));
                  return (
                    <tr key={i} onClick={() => !isUsed && !isEx && setSelectedBankIdx(rIdx)} className={`border-b cursor-pointer transition-all ${isUsed || isEx ? 'bg-emerald-50 text-emerald-600 opacity-60' : selectedBankIdx === rIdx ? 'bg-amber-100 ring-2 ring-inset ring-amber-300' : 'hover:bg-gray-50'}`}>
                      <td className="p-2 text-gray-400 font-mono">{safeGet(row, COL_BANK_DATA) || '-'}</td>
                      <td className="p-2 truncate max-w-[250px] italic">{String(safeGet(row, COL_BANK_DESC))}</td>
                      <td className={`p-2 text-right font-black text-[13px] ${amt < 0 ? 'text-rose-600' : 'text-emerald-700'}`}>{safeGet(row, COL_BANK_IMPORT)}€</td>
                      <td className="p-2 text-center no-print">
                        {!isUsed && (
                          <button onClick={async (e) => {
                            e.stopPropagation(); const n = new Set(bankExclusions); const adding = !n.has(rIdx);
                            const bankHash = row.unique_hash;
                            if (adding) { n.add(rIdx); await saveConciliacio(bankHash, null, 'exclos'); } 
                            else { n.delete(rIdx); await deleteConciliacio(bankHash); }
                            setBankExclusions(n);
                          }} className={`p-1.5 rounded-full transition-colors ${isEx?'bg-emerald-200 text-emerald-700 border-2 border-emerald-400':'text-gray-300 hover:text-emerald-500'}`}><BookmarkCheck size={16}/></button>
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

      {/* RESUM PAIRED */}
      <div id="win-paired" style={{ resize: 'vertical', height: '350px' }} className="w-full bg-white rounded-xl shadow-2xl border-2 border-emerald-300 overflow-auto mb-12 min-h-[150px] flex flex-col">
        <div className="p-3 bg-emerald-600 text-white flex justify-between items-center no-print sticky top-0 z-20">
          <div className="flex items-center gap-4">
            <span className="font-black uppercase text-[10px] tracking-widest text-white">Resum Conciliades</span>
            <div className="flex gap-3 text-[9px] bg-emerald-700 px-3 py-1 rounded-lg">
              <span className="flex items-center gap-1"><span className="text-emerald-200">Total:</span><span className="font-black text-white">{filteredMatchesList.length + filteredCashList.length + filteredBankExclusionsList.length}</span></span>
            </div>
          </div>
          <div className="flex gap-4 items-center">
            <button onClick={() => printSection('win-paired', 'Informe')} className="p-1.5 hover:bg-emerald-500 rounded-full text-white bg-emerald-700 shadow-inner transition"><Printer size={18} /></button>
            <button onClick={() => setShowPaired(!showPaired)} className="p-1 text-white hover:bg-emerald-500 rounded-full"><ChevronDown size={24} className={showPaired ? 'rotate-180' : ''}/></button>
          </div>
        </div>
        {showPaired && (
          <div className="flex-1">
            <table className="w-full text-left text-[9px] border-collapse">
              <thead className="bg-emerald-50 sticky top-0 border-b font-bold uppercase text-emerald-800 z-10">
                <tr>
                  <th className="p-2 w-16 text-center">Tipus</th>
                  <th className="p-2 w-20">Data Fac</th>
                  <th className="p-2">Proveïdor</th>
                  <th className="p-2 text-right border-r">Import</th>
                  <th className="p-2 w-20">Data Banc</th>
                  <th className="p-2 pl-4">Banc / Alerta</th>
                  <th className="p-2 text-right">Import</th>
                  <th className="p-2 no-print text-center w-14">Acció</th>
                </tr>
              </thead>
              <tbody>
                {/* SF */}
                {filteredBankExclusionsList.map((bankIdx, i) => (
                  <tr key={`sf-${i}`} className="border-b bg-orange-50/50 text-orange-900 font-medium">
                    <td className="p-2 font-black text-center uppercase">SF</td>
                    <td className="p-2 text-gray-400" colSpan="3"><span className="italic font-bold text-orange-800">Sense factura</span></td>
                    <td className="p-2 font-mono text-gray-400">{safeGet(bankData[bankIdx], COL_BANK_DATA)}</td>
                    <td className="p-2 pl-4"><span className="italic truncate block">{String(safeGet(bankData[bankIdx], COL_BANK_DESC))}</span></td>
                    <td className="p-2 text-right font-black text-rose-700">{safeGet(bankData[bankIdx], COL_BANK_IMPORT)}€</td>
                    <td className="p-2 text-center no-print"><button onClick={async () => { const bankHash = bankData[bankIdx].unique_hash; const n = new Set(bankExclusions); n.delete(bankIdx); setBankExclusions(n); await deleteConciliacio(bankHash); }} className="text-rose-500 hover:bg-rose-100 p-1.5 rounded-full"><XCircle size={18}/></button></td>
                  </tr>
                ))}
                {/* CASH */}
                {filteredCashList.map((idx, i) => (
                  <tr key={`c-${i}`} className="border-b bg-emerald-50/20 italic text-emerald-900 font-medium">
                    <td className="p-2 font-black text-center uppercase">Cash</td>
                    <td className="p-2 font-mono">{safeGet(invoices[idx], COL_FAC_DATA)}</td>
                    <td className="p-2">{safeGet(invoices[idx], COL_FAC_PROV)}</td>
                    <td className="p-2 text-right font-black border-r">{safeGet(invoices[idx], COL_FAC_TOTAL)}€</td>
                    <td className="p-2 pl-4 text-gray-400 italic" colSpan="3">Pagament fora de circuit bancari</td>
                    <td className="p-2 text-center no-print"><button onClick={async () => { const invHash = invoices[idx].unique_hash; const n = new Set(invoiceCash); n.delete(idx); setInvoiceCash(n); await deleteConciliacio(invHash); }} className="text-rose-500 hover:bg-rose-100 p-1.5 rounded-full"><XCircle size={18}/></button></td>
                  </tr>
                ))}
                {/* BANC */}
                {filteredMatchesList.map(([invIdx, bIdx], i) => (
                  <tr key={`m-${i}`} className="border-b hover:bg-emerald-50 font-medium transition-colors">
                    <td className="p-2 font-black text-blue-800 text-center uppercase">Banc</td>
                    <td className="p-2 font-mono">{safeGet(invoices[invIdx], COL_FAC_DATA)}</td>
                    <td className="p-2 font-bold">{safeGet(invoices[invIdx], COL_FAC_PROV)}</td>
                    <td className="p-2 text-right font-black border-r">{safeGet(invoices[invIdx], COL_FAC_TOTAL)}€</td>
                    <td className="p-2 font-mono">{safeGet(bankData[bIdx], COL_BANK_DATA)}</td>
                    <td className="p-2 pl-4"><span className="italic text-gray-500 truncate block">{String(safeGet(bankData[bIdx], COL_BANK_DESC))}</span></td>
                    <td className="p-2 text-right font-black text-rose-700">{safeGet(bankData[bIdx], COL_BANK_IMPORT)}€</td>
                    <td className="p-2 text-center no-print"><button onClick={async () => { const invHash = invoices[invIdx].unique_hash; const n = {...matches}; delete n[invIdx]; setMatches(n); await deleteConciliacio(invHash); }} className="text-rose-500 hover:bg-rose-100 p-1.5 rounded-full"><Unlink size={18}/></button></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}