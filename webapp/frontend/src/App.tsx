import { Routes, Route, Navigate, useLocation } from "react-router-dom";

import { useConfig, useSession } from "./lib/hooks";
import { ApiError } from "./lib/api";
import AppShell from "./components/AppShell";
import StateScreen from "./components/StateScreen";
import LoginView from "./components/LoginView";
import { AppContextProvider } from "./lib/AppContext";
import DashboardPage from "./pages/DashboardPage";
import ClientsPage from "./pages/ClientsPage";
import ClientEditPage from "./pages/ClientEditPage";
import ClientCreatePage from "./pages/ClientCreatePage";
import BikesPage from "./pages/BikesPage";
import TrainersPage from "./pages/TrainersPage";
import ClientLinksPage from "./pages/ClientLinksPage";
import AdminsPage from "./pages/AdminsPage";
import SchedulePage from "./pages/SchedulePage";
import ScheduleOverviewPage from "./pages/ScheduleOverviewPage";
import InstructorsPage from "./pages/InstructorsPage";
import NotificationsPage from "./pages/NotificationsPage";
import NotificationSettingsPage from "./pages/NotificationSettingsPage";
import MessagingPage from "./pages/MessagingPage";
import MessagesPage from "./pages/MessagesPage";
import ActivitiesPage from "./pages/ActivitiesPage";

import "./styles/layout.css";
import "./styles/messaging.css";

export default function App() {
  const configQuery = useConfig();
  const sessionQuery = useSession();

  if (configQuery.isLoading) {
    return <StateScreen title="Загрузка…" message="Получаем конфигурацию." />;
  }

  if (configQuery.isError || !configQuery.data) {
    return <StateScreen title="Ошибка" message="Не удалось загрузить конфигурацию." />;
  }

  const config = configQuery.data;

  if (sessionQuery.isLoading) {
    return <StateScreen title="Проверка сессии" message="Пожалуйста, подождите…" />;
  }

  if (sessionQuery.isError || !sessionQuery.data) {
    const error = sessionQuery.error as ApiError | undefined;
    if (error?.status === 401) {
      return <LoginView config={config} />;
    }
    return <StateScreen title="Ошибка" message={error?.message ?? "Не удалось загрузить сессию."} />;
  }

  const session = sessionQuery.data;
  const location = useLocation();
  const hideSidebar = location.pathname === "/schedule";

  return (
    <AppContextProvider value={{ session, config }}>
      <AppShell session={session} hideSidebar={hideSidebar}>
        <Routes>
          <Route path="/" element={<Navigate to="/dashboard" replace />} />
          <Route path="/dashboard" element={<DashboardPage />} />
          <Route path="/clients" element={<ClientsPage />} />
          <Route path="/clients/new" element={<ClientCreatePage />} />
          <Route path="/clients/:id" element={<ClientEditPage />} />
          <Route path="/schedule" element={<ScheduleOverviewPage />} />
          <Route path="/schedule/manage" element={<SchedulePage />} />
          <Route path="/schedule/notifications" element={<NotificationsPage />} />
          <Route path="/activities" element={<ActivitiesPage />} />
          <Route path="/schedule/settings" element={<NotificationSettingsPage />} />
          <Route path="/messaging" element={<MessagingPage />} />
          <Route path="/messages" element={<MessagesPage />} />
          <Route path="/instructors" element={<InstructorsPage />} />
          <Route path="/bikes" element={<BikesPage />} />
          <Route path="/trainers" element={<TrainersPage />} />
          <Route path="/links" element={<ClientLinksPage />} />
          <Route path="/admins" element={<AdminsPage />} />
          <Route path="*" element={<Navigate to="/dashboard" replace />} />
        </Routes>
      </AppShell>
    </AppContextProvider>
  );
}