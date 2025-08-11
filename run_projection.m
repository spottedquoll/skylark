
disp('Running projection of COMTRADE data pipeline...');

%% Initialise

% Set current directory
current_dir = [fileparts(which(mfilename)) filesep]; 
addpath(genpath(current_dir));

% Environment and settings
options = json_parser([current_dir 'config.json']);
meta = initialise_environment(options);

conc_dir = meta.conc_dir;
save_dir = meta.save_dir;

% Settings
flows = {'Import', 'Export'};
trade_units = {'$_CIF', '$_FOB', 'kg'};
timeseries = options.timeseries;
comtrade_dir = options.env.comtrade_dir;
base_classification = 'HS17';
prealloc_guess = round(n_records*3);

%% Concordances

% Country concordances
[~,~,countries_iso_numeric] = xlsread([conc_dir '/countries-iso-numeric-m49-conc.xlsx']);
iso_code = cell2mat(countries_iso_numeric(2:end,2));
iso_alpha = countries_iso_numeric(2:end,3);

[~,~,countries_iso_numeric] = xlsread([conc_dir '/reporters.xlsx']);
iso_code = cell2mat(countries_iso_numeric(2:end,3));
iso_alpha = countries_iso_numeric(2:end,7);

country_acronyms = meta.root_country_legend(2:end,2);
assert(size(country_acronyms,1) == meta.n_reg_root_gl);

% HS concordances
[~,~,hs_version_conc] = xlsread([conc_dir '/HS-SITC-BEC_Correlations_2022_bis.xlsx']);

vs_header = hs_version_conc(1,1:7);
hs_version_conc = hs_version_conc(2:end-2,1:7);
stable_2017_col = find(strcmp(vs_header,'HS17'));
hs_2017_commodities = unique(hs_version_conc(:,stable_2017_col));

[~,~,hs_version_year] = xlsread([conc_dir '/hs_version_year.xlsx']);

% Stores
hs_prematch = zeros(1,2+length(hs_2017_commodities));

%% Unpack by year
for t = min(timeseries) : max(timeseries)

    disp([' unpacking ' num2str(t) '...']);

    % Read from cache or build from scratch
    save_fname = [save_dir 'comtrade-tensor-' base_classification '-' num2str(t) '.mat'];
    if isfile(save_fname)
    
        disp([' skipping ' num2str(t) '.']); 
    
    else
    
        % Read raw data
        fname = [comtrade_dir 'comtrade-finished/' 'comtrade-joined-records-' num2str(t) '.csv'];
        assert(isfile(fname));

        T = readtable(fname);
    
        header = T.Properties.VariableNames;
        trade = table2cell(T);
    
        clear T

        % Log stores
        missing_cs = {};
        missing_countries = [];
    
        % Define column names 
        col_names = [{'classificationCode', 'hs_version'}; ...
            {'cmdCode', 'commodity_code'}; ...
            {'reporterCode', 'reporter_code'}; ...
            {'partnerCode', 'partner_code'}; ...
            {'netWgt', 'weight'}; ...
            {'FOBValue', 'value_fob'}; ...
            {'CIFValue', 'value_cif'}; ...
            {'flowCode', 'flow'}; ...
        ];
    
        % Index column names
        col_idx = struct();
        
        for c = 1:size(col_names,1)
            match = find(strcmp(header,col_names{c,1}));
            assert(~isempty(match) && length(match) == 1);
            col_idx.(col_names{c,2}) = match;
        end
    
        % Filter records
        n_raw = size(trade,1);
    
        tmp = cell2mat(trade(:,[col_idx.value_fob col_idx.value_cif col_idx.weight col_idx.reporter_code col_idx.partner_code]));

        a = union(union(find(~isnan(tmp(:,1))), find(~isnan(tmp(:,2)))), find(~isnan(tmp(:,3))));
        b = union(find(strcmp(trade(:,col_idx.flow),'X')),find(strcmp(trade(:,col_idx.flow),'M')));
        c = union(union(find(tmp(:,1) > 0), find(tmp(:,2) > 0)), find(tmp(:,3) > 0));
        g = intersect(find(tmp(:,4) > 0), find(tmp(:,5) > 0));
        d = intersect(intersect(a,b),intersect(c,g));
    
        e = find(ismember(header,col_names(:,1)));
    
        trade = trade(d,e);
        header = header(e);
    
        clear a b c d e tmp
    
        disp(['  discovered ' thousands_separated(size(trade,1)) ' valid records (' num2str(round(size(trade,1)/n_raw*10000)/100) '%)']);
        
        % Re-index column names
        col_idx = struct();
        
        for c = 1:size(col_names,1)
            match = find(strcmp(header,col_names{c,1}));
            assert(~isempty(match) && length(match) == 1);
            col_idx.(col_names{c,2}) = match;
        end
    
        % Store in a sparse array: {origin, destination, commodity, mode, recorded_direction}
        n_records = size(trade,1);
        edge_dims = [size(country_acronyms,1), size(country_acronyms,1), size(hs_2017_commodities,1), size(flows,2), length(trade_units)];
    
        subs = zeros(prealloc_guess,size(edge_dims,2)); 
        vals = zeros(prealloc_guess,1); 
    
        % Extract each line
        j = 1; logging = round(linspace(1,n_records,40));
    
        for i = 1:n_records
    
            row = trade(i,:);
            new_entry = zeros(1,size(subs,2));
    
            % Match country names (country ISO numeric to acronym)
            reporter_idx = [];
            try 
                reporter_alpha = iso_alpha(find(iso_code == row{col_idx.reporter_code}));
                reporter_idx = find(strcmp(reporter_alpha, country_acronyms));
            catch
                if ~ismember(row{col_idx.reporter_code},missing_countries)
                    missing_countries = [missing_countries; row{col_idx.reporter_code}];
                end
            end
    
            partner_idx = [];
            try            
                partner_alpha = iso_alpha(find(iso_code == row{col_idx.partner_code}));
                partner_idx = find(strcmp(partner_alpha, country_acronyms));
            catch
                if ~ismember(row{col_idx.partner_code},missing_countries)
                    missing_countries = [missing_countries; row{col_idx.partner_code}];
                end
            end
    
            if ~isempty(reporter_idx) && ~isempty(partner_idx)
                
                % Determine flow direction
                if strcmp(row(col_idx.flow), 'X')
                    fl = 'Export';
                elseif strcmp(row(col_idx.flow), 'M')
                    fl = 'Import';
                else
                    error('Unknown flow direction.');
                end
    
                % Flow origin-destination depends on flow direction
                flow_idx = find(strcmp(fl,flows), 1); 
                assert(~isempty(flow_idx));
    
                new_entry(4) = flow_idx;

                % Classification year
                hs_year_match = hs_version_year(find(strcmp(hs_version_year(:,1),row(col_idx.hs_version))),2);
                hs_year = num2str(cell2mat(hs_year_match));
                assert(~isempty(hs_year));
    
                % Interpret HS commodity code
                raw_hs_code = row{col_idx.commodity_code};
                
                preindexed = intersect(find(hs_prematch(:,1) == str2double(hs_year)), find(hs_prematch(:,2) == raw_hs_code));
                if ~isempty(preindexed)
                    c_idx_hs6 = find(hs_prematch(preindexed,3:end));
                    assert(length(preindexed) == 1);
                else

                    c_idx_hs6 = [];
                    
                    % Format HS code
                    hs6_code = num2str(row{col_idx.commodity_code});
                    if length(hs6_code) < 6; hs6_code = ['0' hs6_code]; end
                    assert(length(hs6_code) == 6);
                    
                    % Cast commodity codes to HS 2017
                    hs_conc_col = find(strcmp(vs_header,['HS' hs_year(3:end)]));
                    hs_conc_row = find(strcmp(hs_version_conc(:,hs_conc_col), hs6_code));
        
                    if isempty(hs_conc_row) 
                        z = 1;
                        while isempty(hs_conc_row) && z <= size(hs_version_conc,2)
                            hs_conc_row = find(strcmp(hs_version_conc(:,z), hs6_code));
                            z = z + 1;
                        end
                    end
    
                    % Was an HS match possible
                    if ~isempty(hs_conc_row) && ~isempty(hs_conc_col)
                        
                        hs17_match = unique(hs_version_conc(hs_conc_row,stable_2017_col));
                        for k = 1:size(hs17_match,1)

                            match = hs17_match(k);
    
                            c_idx_hs6_k = find(strcmp(hs_2017_commodities, char(match)));
                            assert(~isempty(c_idx_hs6_k) && length(c_idx_hs6_k) == 1);

                            c_idx_hs6 = [c_idx_hs6 c_idx_hs6_k];
                        
                        end                        
                    end

                    % Save match
                    if ~isempty(c_idx_hs6)
                        assert(no_duplicates(c_idx_hs6));
                        zs = zeros(1,length(hs_2017_commodities));
                        zs(c_idx_hs6) = 1;
                        hs_prematch = [hs_prematch; [str2double(hs_year) raw_hs_code zs]];  
                    end

                end

                % Write record
                if isempty(c_idx_hs6)
                    missing_cs = [missing_cs; [hs6_code ' (' row{col_idx.hs_version} ')']];
                else

                    % Populate new record                    
                    if strcmp(fl,'Import')
                        new_entry(1) = partner_idx;
                        new_entry(2) = reporter_idx;
                    elseif strcmp(fl,'Export')
                        new_entry(1) = reporter_idx;
                        new_entry(2) = partner_idx;
                    end

                    % Write a fractional record for every HS match
                    n_matches = size(c_idx_hs6,2);
                    assert(size(c_idx_hs6,1) == 1);

                    % Weight (kg)
                    if ~isnan(row{col_idx.weight}) && row{col_idx.weight} > 0
                        
                        weight = row{col_idx.weight};
                        assert(~isnan(weight) && isfinite(weight) && weight > 0);

                        new_entry(5) = 3;

                        % Write one record or apportion over many
                        if n_matches == 1

                            % Build new record 
                            new_entry(3) = c_idx_hs6;

                            % Add to store
                            assert(isempty(find(new_entry == 0, 1)),'Address vector is incomplete');
                            subs(j,:) = new_entry;
                            vals(j) = weight;
                            j = j + 1;

                        else

                            % Build new record 
                            new_entries = repmat(new_entry,n_matches,1);
                            new_entries(:,3) = c_idx_hs6;
                            val_vec = repmat(weight/n_matches,n_matches,1);

                            assert(~any(new_entries(:) == 0),'Address vector is incomplete');

                            % Add to store
                            subs(j:j+n_matches-1,:) = new_entries;
                            vals(j:j+n_matches-1) = val_vec;
                            j = j + n_matches;

                        end
                    end

                    % Monetary value
                    if row{col_idx.value_fob} > 0 || row{col_idx.value_cif} > 0 

                        % Unpack value field
                        value = -1;
                        if flow_idx == 1
                            if ~isnan(row{col_idx.value_cif}) && row{col_idx.value_cif} > 0 
                                value = row{col_idx.value_cif};
                                unit = 1;
                            elseif ~isnan(row{col_idx.value_fob}) && row{col_idx.value_fob} > 0 
                                value = row{col_idx.value_fob};
                                unit = 2;
                            end
                        elseif flow_idx == 2 
                            if ~isnan(row{col_idx.value_fob}) && row{col_idx.value_fob} > 0 
                                value = row{col_idx.value_fob};
                                unit = 2;
                            end
                        else
                            error(['Unknown flow ' num2str(flow_idx)]);
                        end

                        assert(~isnan(value) && isfinite(value));
                        new_entry(5) = unit;

                        % Write one record or apportion over many
                        if value > 0
                            if n_matches == 1
    
                                % Build new record 
                                new_entry(3) = c_idx_hs6;
    
                                % Add to store
                                assert(isempty(find(new_entry == 0, 1)),'Address vector is incomplete');
                                subs(j,:) = new_entry;
                                vals(j) = weight;
                                j = j + 1;
    
                            else
    
                                % Build new record 
                                new_entries = repmat(new_entry,n_matches,1);
                                new_entries(:,3) = c_idx_hs6;
                                val_vec = repmat(value/n_matches,n_matches,1);
    
                                assert(~any(new_entries(:) == 0),'Address vector is incomplete');
    
                                % Add to store
                                subs(j:j+n_matches-1,:) = new_entries;
                                vals(j:j+n_matches-1) = val_vec;
                                j = j + n_matches;
    
                            end
                        end
                    end

                end
            end
            
            % Log progress
            if ~isempty(find(logging == i, 1))
                disp(['  completed ' num2str(round(i/n_records*100)) '%']);
            end
            
            % Check store size
            if j >= size(subs,1)
                disp(['  expanding store size ' thousands_separated(size(subs,1)) ' -> ' thousands_separated(1.25*size(subs,1))]);
                n_extra = round(0.25*size(subs,1));
                subs = [subs; zeros(n_extra, size(subs,2))]; 
                vals = [vals; zeros(n_extra, size(vals,2))];
            end
                                
        end
    
        clear trade
    
        disp('  converting to sparse tensor...');
    
        % Trim to size
        if j < size(subs,1)
            subs(j:end,:) = [];
            vals(j:end,:) = [];
            disp(['  pre-allocation guess: ' ThousandSep(prealloc_guess) ', final size: ' ThousandSep(j)]);
        end
    
        assert(all_finite(vals) && all_positive(vals));
        assert(size(vals,1) == size(subs,1));
        assert(size(subs,2) == size(edge_dims,2));
    
        % Save as sparse tensor: {origin, destination, recorded_direction, commodity}
        trade_tensor.data = sptensor(subs,vals(:,1),edge_dims); 

        trade_tensor.meta.edge_dims = edge_dims; 
        trade_tensor.meta.flows = flows;
        trade_tensor.meta.edges = {'origin', 'destination', 'commodity', 'flow', 'unit'};
        trade_tensor.meta.units = {'$_CIF', '$_FOB', 'kg'};
    
        % Write tensor to disk
        disp('  writing to disk...');

        save(save_fname, 'trade_tensor', '-v7.3');

        % Log missing commodities
        disp(['Could not match: ' num2str(size(missing_cs,1)) ' records.']);

        fname = [save_dir 'unmatched-hs-codes-' num2str(t) '.mat'];
        save(fname,'missing_cs');

    end

end

%% Finish

% Write matches shortcut
fname = [conc_dir 'all-hs-matches.mat'];
hs_match_conc = hs_prematch;
save(fname,'hs_match_conc');

disp('Complete.');