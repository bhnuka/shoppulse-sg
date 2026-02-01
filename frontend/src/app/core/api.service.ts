import { Injectable } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';

export interface OverviewMetric {
  label: string;
  value: number | string;
  change_yoy_pct?: number | null;
}

export interface OverviewResponse {
  period_start: string;
  period_end: string;
  metrics: OverviewMetric[];
  top_ssic?: { ssic_code?: string | null; count: number } | null;
  hottest_planning_area?: { planning_area_id?: string | null; count: number } | null;
}

export interface TrendPoint {
  month: number;
  count: number;
}

export interface TrendsResponse {
  series: TrendPoint[];
}

export interface RankingItem {
  ssic_code?: string | null;
  ssic_description?: string | null;
  count: number;
}

export interface RankingsResponse {
  items: RankingItem[];
}

export interface MapHotspot {
  subzone_id?: string | null;
  name?: string | null;
  planning_area_id?: string | null;
  count: number;
  geometry: string;
}

export interface MapHotspotsResponse {
  month_start: number;
  month_end: number;
  hotspots: MapHotspot[];
}

export interface EntitySummary {
  uen: string;
  entity_name: string;
  entity_status_description: string;
  entity_type_description: string;
  business_constitution_description: string;
  company_type_description?: string | null;
  registration_incorporation_date?: string | null;
  uen_issue_date?: string | null;
  primary_ssic_code?: string | null;
  secondary_ssic_code?: string | null;
  postal_code?: string | null;
  planning_area_id?: string | null;
  subzone_id?: string | null;
}

export interface EntityDetail extends EntitySummary {
  primary_ssic_norm?: string | null;
  secondary_ssic_norm?: string | null;
  latitude?: number | null;
  longitude?: number | null;
}

export interface EntitySearchResponse {
  total: number;
  items: EntitySummary[];
}

export interface ChatSqlResponse {
  intent: string;
  slots: Record<string, unknown>;
  sql: string;
}

export interface ChatResponse extends ChatSqlResponse {
  data: Record<string, unknown>[];
  narrative: string;
}

@Injectable({ providedIn: 'root' })
export class ApiService {
  private readonly baseUrl = 'http://localhost:8000';

  constructor(private readonly http: HttpClient) {}

  getOverview(params?: { from?: string; to?: string; ssic?: string; area?: string; area_type?: string }): Observable<OverviewResponse> {
    let httpParams = new HttpParams();
    Object.entries(params ?? {}).forEach(([key, value]) => {
      if (value) {
        httpParams = httpParams.set(key, value);
      }
    });
    return this.http.get<OverviewResponse>(`${this.baseUrl}/api/overview`, { params: httpParams });
  }

  getTrends(params?: { ssic?: string; area?: string; area_type?: string; from?: string; to?: string }): Observable<TrendsResponse> {
    let httpParams = new HttpParams();
    Object.entries(params ?? {}).forEach(([key, value]) => {
      if (value) {
        httpParams = httpParams.set(key, value);
      }
    });
    return this.http.get<TrendsResponse>(`${this.baseUrl}/api/trends/new-entities`, { params: httpParams });
  }

  getTopSsic(params?: { from?: string; to?: string; area?: string; area_type?: string; limit?: number }): Observable<RankingsResponse> {
    let httpParams = new HttpParams();
    Object.entries(params ?? {}).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== '') {
        httpParams = httpParams.set(key, String(value));
      }
    });
    return this.http.get<RankingsResponse>(`${this.baseUrl}/api/rankings/top-ssic`, { params: httpParams });
  }

  getHotspots(params?: { ssic?: string; from?: string; to?: string }): Observable<MapHotspotsResponse> {
    let httpParams = new HttpParams();
    Object.entries(params ?? {}).forEach(([key, value]) => {
      if (value) {
        httpParams = httpParams.set(key, value);
      }
    });
    return this.http.get<MapHotspotsResponse>(`${this.baseUrl}/api/map/hotspots`, { params: httpParams });
  }

  searchEntities(params: { q?: string; ssic?: string; status?: string; limit?: number; offset?: number }): Observable<EntitySearchResponse> {
    let httpParams = new HttpParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== '') {
        httpParams = httpParams.set(key, String(value));
      }
    });
    return this.http.get<EntitySearchResponse>(`${this.baseUrl}/api/entities/search`, { params: httpParams });
  }

  getEntity(uen: string): Observable<EntityDetail> {
    return this.http.get<EntityDetail>(`${this.baseUrl}/api/entities/${uen}`);
  }

  chatSql(question: string): Observable<ChatSqlResponse> {
    return this.http.post<ChatSqlResponse>(`${this.baseUrl}/api/chat/sql-only`, { question });
  }

  chatQuery(question: string): Observable<ChatResponse> {
    return this.http.post<ChatResponse>(`${this.baseUrl}/api/chat/query`, { question });
  }
}
