## WHG v3 Tests 
_as of 17 November 2026: the stale tests were repaired or removed (see git log); not every test file is listed here_

### tests/test_place_collections.py
#### 1. [CollectionTestCase(PlaceCollection)](tests/test_place_collections.py)
    
    - test_add_places (sets)
    - test_add_dataset_places (all of a dataset)
    - test_add_and_remove_dataset_places

### tests/test_collectiongroups.py
#### 2. [CollectionGroupTestCase](tests/test_collectiongroups.py)
    - test_only_group_leader_can_create_collection_group
    - test_add_user_to_collection_group
    - test_submit_collection_to_group
    - test_withdraw_collection_from_group

#### 3. [CollectionGroupScenarioTestCase](tests/test_collectiongroups.py)
    - test_submit_collection_to_group
      - check, uncheck 'reviewed' flag
      - check, uncheck 'nominated' flag
    - test_withdraw_collection_from_group

### tests/test_models.py
#### 4. [DatasetAndPlaceModelsTest](tests/test_models.py)
    - test_dataset
    - test_place

### 5. [DatasetCreateViewTest (validation: upload, insert)](tests/test_validation.py)
    - test_validate_delim
    - test_ds_insert_delim (fails on missing_field.tsv)
    - test_end_to_end (fails)

### 6. [ReconWD](tests/test_wdlocal.py)
    - test_ReconWDlocal (fails)

### tests/test_tilesets.py
#### 7. [TestSendTilesetRequestIntegration](tests/tilesets.py)
    - test_send_tileset_request

### tests/test_emailing.py
#### 8. [NewUserTestCase](tests/test_emailing.py)
    - test_new_user_emails

#### 9. [DatasetSignalTestCase](tests/test_emailing.py)
    - test_send_new_dataset_email
    - test_handle_public_true
    - test_handle_public_false
    - test_handle_wdcomplete
    - test_handle_indexed

#### 10. [ContactFormTestCase](tests/test_emailing.py)
    - test_contact_form_goes_to_zulip

### 11. [DumpDistinctToponymsCommandTest](tests/test_dumps.py)
    - test_command_output_zip

### tests/test_reviewer.py
#### 12. [PlaceGeomPlaceLinkTest](tests/test_reconciliation.py)
    - test_create_placegeom_with_task_id_without_reviewer
    - test_create_placelink_with_task_id_without_reviewer
    - test_create_placegeom_with_task_id
    - test_create_placelink_with_task_id

### tests/test_migration_password_change.py
#### 13. [MigrationPwdTest](tests/tests/test_migration_password_change.py)
    - test_login_redirects_when_must_change_password
    - test_normal_login

### tests/test_no_geonames.py
#### 14. [TestExcludeGeonames](tests/tests/test_nogeonames.py)
    - test_exclude_geonames_effectiveness






