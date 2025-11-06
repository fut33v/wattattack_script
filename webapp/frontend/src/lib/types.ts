export interface TelegramUser {
  id: number;
  username?: string | null;
  first_name?: string | null;
  last_name?: string | null;
  display_name?: string;
  photo_url?: string | null;
}

export interface SessionResponse {
  user: TelegramUser;
  isAdmin: boolean;
}

export interface ConfigResponse {
  loginBotUsername: string;
  clientsPageSize: number;
  baseUrl?: string | null;
}

export interface Pagination {
  page: number;
  pageSize: number;
  total: number;
  totalPages: number;
}

export interface ClientRow {
  id: number;
  first_name?: string | null;
  last_name?: string | null;
  full_name?: string | null;
  gender?: string | null;
  weight?: number | null;
  height?: number | null;
  ftp?: number | null;
  pedals?: string | null;
  goal?: string | null;
  saddle_height?: string | null;
  favorite_bike?: string | null;
  submitted_at?: string | null;
}

export interface ClientListResponse {
  items: ClientRow[];
  pagination: Pagination;
}

export interface BikeRow {
  id: number;
  title: string;
  owner?: string | null;
  size_label?: string | null;
  frame_size_cm?: string | null;
  height_min_cm?: number | null;
  height_max_cm?: number | null;
  gears?: string | null;
  axle_type?: string | null;
  cassette?: string | null;
}

export interface BikeListResponse {
  items: BikeRow[];
}

export interface TrainerRow {
  id: number;
  code?: string | null;
  title?: string | null;
  display_name?: string | null;
  owner?: string | null;
  axle_types?: string | null;
  cassette?: string | null;
  notes?: string | null;
  bike_id?: number | null;
  bike_title?: string | null;
  bike_owner?: string | null;
}

export interface TrainerListResponse {
  items: TrainerRow[];
}

export interface AdminRow {
  id: number;
  tg_id?: number | null;
  username?: string | null;
  display_name?: string | null;
  created_at?: string | null;
}

export interface AdminListResponse {
  items: AdminRow[];
}

export interface ClientLinkRow {
  client_id: number;
  tg_user_id: number;
  tg_username?: string | null;
  tg_full_name?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
}

export interface ClientLinkListResponse {
  items: ClientLinkRow[];
}

export interface ScheduleWeekRow {
  id: number;
  week_start_date: string;
  title?: string | null;
  notes?: string | null;
  copied_from_week_id?: number | null;
  created_at?: string | null;
  updated_at?: string | null;
  slots_count?: number | null;
}

export interface ScheduleWeekListResponse {
  items: ScheduleWeekRow[];
  pagination: Pagination;
}

export interface ScheduleReservation {
  id: number;
  slot_id: number;
  stand_id?: number | null;
  stand_code?: string | null;
  client_id?: number | null;
  client_name?: string | null;
  status: string;
  source?: string | null;
  notes?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
}

export interface ScheduleSlot {
  id: number;
  week_id: number;
  slot_date: string;
  start_time: string;
  end_time: string;
  label?: string | null;
  session_kind: string;
  is_cancelled?: boolean;
  sort_index?: number | null;
  notes?: string | null;
  reservations: ScheduleReservation[];
  instructorId?: number | null;
  instructorName?: string | null;
}

export interface ScheduleStandSummary {
  id: number;
  code?: string | null;
  display_name?: string | null;
  title?: string | null;
}

export interface ScheduleWeekDetailResponse {
  week: ScheduleWeekRow;
  slots: ScheduleSlot[];
  stands: ScheduleStandSummary[];
  instructors: InstructorRow[];
}

export interface InstructorRow {
  id: number;
  full_name: string;
  created_at?: string | null;
}

export interface InstructorListResponse {
  items: InstructorRow[];
}

export interface FillTemplateResponse {
  created: number;
  slots: ScheduleSlot[];
}
