import { TestBed, inject } from '@angular/core/testing';
import { HttpClientModule } from '@angular/common/http';
import { RouterTestingModule } from '@angular/router/testing';
import { of } from 'rxjs';
import { beforeEach, describe, expect, it } from '@jest/globals';

import { HistoryService } from './history.service';

describe('HistoryService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientModule, RouterTestingModule],
      providers: [HistoryService],
    });
  });

  it('should be ready', inject([HistoryService], (service: HistoryService) => {
    expect(service).toBeTruthy();
  }));

  it('reads offline times next to session starts', inject(
    [HistoryService],
    (service: HistoryService) => {
      (service as any).request = () =>
        of({
          listFields: {
            HISTORY: ['{DATE=x, SESSION=s1, TIME=1000}'],
            OFFLINE: ['2026-10-27T08:21:42:114', 'bad'],
          },
        });
      let rows: any[] = [];
      service.getInstanceHistory('C', 'i').subscribe((r) => (rows = r));
      expect(rows.map((r) => r.event)).toEqual(['Online', 'Offline']);
      expect(rows[1].time).toBe(Date.UTC(2026, 9, 27, 8, 21, 42, 114));
    },
  ));
});
