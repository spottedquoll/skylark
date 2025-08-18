
disp('Running COMTRADE diagnostics...');

%% Initialise

% Set current directory
current_dir = [fileparts(which(mfilename)) filesep]; 
addpath(genpath(current_dir));

% Environment and settings
options = json_parser([current_dir 'config.json']);
meta = initialise_environment(options);

% Paths
conc_dir = meta.conc_dir;

save_dir = [options.env.comtrade_dir 'diagnostics/'];
if ~isfolder(save_dir)
    mkdir(save_dir);
end

% Settings
n_attempts = 100;
t = 2017;
n_records = 25;

%% Find records with both CIF and FOB

% Read raw data
fname = [options.env.comtrade_dir 'comtrade-finished/' 'comtrade-joined-records-' num2str(t) '.csv'];
assert(isfile(fname));

T = readtable(fname);

% Drop records
T = removevars(T,['isQtyEstimated', 'altQtyUnitCode', 'altQty', 'isAltQtyEstimated', "isNetWgtEstimated", ...
                "isGrossWgtEstimated", 'legacyEstimationFlag', 'isReported', 'isAggregate']);

% Convert to array
header = T.Properties.VariableNames;
trade = table2cell(T);

clear T

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

tmp = cell2mat(trade(:,[col_idx.value_fob col_idx.value_cif col_idx.reporter_code col_idx.partner_code col_idx.commodity_code]));

a = intersect(find(~isnan(tmp(:,1))), find(~isnan(tmp(:,2)))); % both CIF and FOB values
b = find(strcmp(trade(:,col_idx.flow),'M')); % reported as import
c = intersect(find(tmp(:,1) > 0), find(tmp(:,2) > 0)); % both imports and exports are non-zero
d = intersect(intersect(a,b), c);

d = d(randperm(length(d)));

% Look for matching export
i = 1;
n_pairs = 1;
report = {};

while i <= n_attempts && n_pairs < n_records && i <= length(d)

    % Import row with matching properties
    row = trade(d(i),:);

    % Get partner record (swap reporter and partner)
    e = intersect(find(tmp(:,3) == row{col_idx.partner_code}),find(tmp(:,4) == row{col_idx.reporter_code}));
    
    % Export records
    f = intersect(find(strcmp(trade(:,col_idx.flow),'X')), find(~isnan(tmp(:,1))));

    % Commodity code
    g = intersect(e,find(tmp(:,5) == row{col_idx.commodity_code}));

    % Matching records
    h = intersect(f,g);

    if ~isempty(h)
        disp([num2str(i) '-' num2str(length(h)) ', ' num2str(n_pairs) '/' n_records]);
        if size(h,1) == 1
            report = [report; [n_pairs row]];
            report = [report; [n_pairs trade(h,:)]];
            n_pairs = n_pairs + 1;
        end
    end

    i = i + 1;

end

% Save
report = [['pairing' header]; report];
fname = [save_dir 'cif-fob-paired-records.xlsx'];
write_cell_to_disk(report,fname);

disp('Finished.');