# Legacy Code Cleanup - Social Module

## Overview

This document identifies legacy code that has been replaced by the new SOLID-compliant architecture.

---

## Files to Delete

### ❌ Legacy Scripts (`social/scripts/`)

**Status**: REPLACED by `social/run_pipeline.py`

- `social/scripts/run_linkedin_ads.py` - Replaced by LinkedInAdsAdapter
- `social/scripts/run_google_ads.py` - Will be replaced by GoogleAdsAdapter
- `social/scripts/test_google_ads.py` - Old test script
- `social/scripts/__init__.py` - Not needed

**Reason**: All functionality moved to:
- `social/run_pipeline.py` (main entry point)
- `social/adapters/linkedin_adapter.py` (LinkedIn logic)
- `social/adapters/google_adapter.py` (Google logic - TODO)

### ❌ Legacy Platform Implementations

#### LinkedIn (`social/platforms/linkedin/`)

**Keep temporarily for reference**:
- `config_linkedin_ads.yml` - Still needed by new config system
- `__init__.py` (contains company_account mapping)

**Can delete after verification**:
- `ads_client.py` - Replaced by `LinkedInHTTPClient` + `LinkedInAdsAdapter`
- `processor.py` - Replaced by processing strategies
- `endpoints.py` - Endpoint definitions moved to config YAML
- `noquotedsession.py` - Integrated into `LinkedInHTTPClient`

#### Google (`social/platforms/google/`)

**Keep for now**:
- `config_google_ads.yml` - Needed by config system
- `ads_client.py` - Reference for Google Ads adapter implementation
- `processor.py` - Reference for processing strategies
- `fields.py` - Reference for field definitions
- `__init__.py` - Contains account mappings

#### Facebook (`social/platforms/facebook/`)

**Can delete entirely**:
- All files - Facebook integration not in scope

---

## Repository Layer

### ⚠️  Keep but Deprecate (`social/repository/`)

**Files**:
- `social_repository.py` - Legacy repository implementation
- `templatesql.py` - SQL templates

**Status**: REPLACED by `social/infrastructure/database.py`

**Action**: Keep for now as fallback, mark as deprecated, delete after full testing

**Reason**: New `VerticaDataSink` provides cleaner interface and better error handling

---

## Utility Files

### ❓ Review (`social/utils/`)

**Files to check**:
- `commons.py` - May contain utility functions used elsewhere

**Action**: Review usage, migrate useful functions to appropriate modules, then delete

---

## Configuration Files

### ✅ Keep

- `social/platforms/linkedin/config_linkedin_ads.yml` - Used by new ConfigurationManager
- `social/platforms/google/config_google_ads.yml` - Used by new ConfigurationManager

### ❌ Delete

- Any old JSON or Python-based config files

---

## Cleanup Steps

### Phase 1: Safe Deletions (Immediate)

```bash
# Delete Facebook platform (not in scope)
rm -rf social/platforms/facebook/

# Delete old test scripts
rm social/scripts/test_google_ads.py
```

### Phase 2: After LinkedIn Testing (1-2 weeks)

Once LinkedIn adapter is fully tested and working in production:

```bash
# Delete legacy LinkedIn implementation
rm social/platforms/linkedin/ads_client.py
rm social/platforms/linkedin/processor.py
rm social/platforms/linkedin/endpoints.py
rm social/platforms/linkedin/noquotedsession.py

# Delete LinkedIn test script
rm social/scripts/run_linkedin_ads.py

# Keep only config and __init__ (for company mapping)
```

### Phase 3: After Google Ads Migration (2-4 weeks)

Once Google Ads adapter is implemented and tested:

```bash
# Delete Google test script
rm social/scripts/run_google_ads.py

# Delete entire scripts directory
rm -rf social/scripts/

# Migrate Google platform code (similar to LinkedIn)
```

### Phase 4: Repository Cleanup (4-6 weeks)

After all platforms use new infrastructure:

```bash
# Delete legacy repository
rm -rf social/repository/
```

### Phase 5: Final Cleanup (6-8 weeks)

After full production validation:

```bash
# Delete platforms directory entirely
rm -rf social/platforms/

# Move config files to top level
mv social/platforms/linkedin/config_linkedin_ads.yml social/config/
mv social/platforms/google/config_google_ads.yml social/config/
```

---

## Migration Checklist

### LinkedIn Platform

- [x] HTTP client migrated to `LinkedInHTTPClient`
- [x] Data processing migrated to processing strategies
- [x] Authentication migrated to `DatabaseTokenProvider`
- [x] Database operations migrated to `VerticaDataSink`
- [x] Adapter implemented as `LinkedInAdsAdapter`
- [ ] End-to-end testing completed
- [ ] Production deployment successful
- [ ] Legacy code deleted

### Google Ads Platform

- [ ] HTTP client created
- [ ] Data processing strategies created
- [ ] Authentication integrated
- [ ] Adapter implemented as `GoogleAdsAdapter`
- [ ] End-to-end testing completed
- [ ] Production deployment successful
- [ ] Legacy code deleted

---

## Code Size Reduction

### Before Refactoring

```
social/
├── scripts/          (~1200 lines)
├── platforms/        (~2500 lines)
│   ├── linkedin/     (~1200 lines)
│   ├── google/       (~1000 lines)
│   └── facebook/     (~300 lines)
├── repository/       (~400 lines)
└── utils/            (~300 lines)

Total: ~4400 lines of legacy code
```

### After Refactoring

```
social/
├── core/             (~600 lines - NEW)
├── domain/           (~500 lines - NEW)
├── adapters/         (~800 lines - NEW)
├── infrastructure/   (~400 lines - NEW)
├── processing/       (~600 lines - NEW)
└── run_pipeline.py   (~400 lines - NEW)

Total: ~3300 lines of new, clean code
```

**Result**: 25% code reduction + significantly better quality

---

## Benefits of Cleanup

1. **Reduced Complexity**: Single, clear entry point (`run_pipeline.py`)
2. **No Duplication**: DRY principle applied
3. **Better Testing**: Mock-friendly architecture
4. **Clearer Dependencies**: Explicit dependency injection
5. **Easier Maintenance**: SOLID principles followed
6. **Faster Onboarding**: Clear structure and documentation

---

## Risk Mitigation

### Keep Backups

Before any deletion:
1. Ensure code is committed to Git
2. Tag current version: `git tag -a v1-legacy-backup -m "Backup before cleanup"`
3. Create backup branch: `git checkout -b backup-legacy-code`

### Gradual Migration

- **Don't delete everything at once**
- Test each platform thoroughly before removing legacy code
- Keep legacy code accessible for 1-2 months after new code is in production
- Monitor production metrics to ensure no regressions

### Rollback Plan

If issues arise:
1. Legacy code available in Git history
2. Can revert to specific commits
3. Docker images tagged with versions
4. Database queries logged for comparison

---

## Verification Before Deletion

For each file before deletion, verify:

1. ✅ Functionality migrated to new architecture
2. ✅ New code tested (unit + integration)
3. ✅ New code deployed to production
4. ✅ Production metrics stable for 2+ weeks
5. ✅ No references in other modules
6. ✅ Documentation updated

---

## Notes

- Keep `platforms/linkedin/__init__.py` for company_account mapping until migrated to config/database
- Keep `platforms/google/__init__.py` for same reason
- Consider migrating company mappings to database table or YAML config
- Review `utils/commons.py` for any widely-used utilities before deletion
