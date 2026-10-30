import React from 'react'
import ReactDOM from 'react-dom/client'
import { BrowserRouter, Routes, Route, NavLink } from 'react-router-dom'
import './index.css'
import { Hosts } from './pages/Hosts'
import { HostDetail } from './pages/HostDetail'

function Layout() {
  return (
    <div className="layout">
      <header className="topbar">
        <span>🫀</span>
        <h1>PulseWatch</h1>
        <nav>
          <NavLink to="/" end className={({ isActive }) => isActive ? 'active' : ''}>
            Hosts
          </NavLink>
        </nav>
      </header>
      <main>
        <Routes>
          <Route path="/" element={<Hosts />} />
          <Route path="/hosts/:hostId" element={<HostDetail />} />
        </Routes>
      </main>
    </div>
  )
}

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <BrowserRouter>
      <Layout />
    </BrowserRouter>
  </React.StrictMode>,
)
