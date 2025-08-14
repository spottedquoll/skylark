
disp('Running COMTRADE diagnostics...');

%% Initialise

% Set current directory
current_dir = [fileparts(which(mfilename)) filesep]; 
addpath(genpath(current_dir));

% Environment and settings
options = json_parser([current_dir 'config.json']);
meta = initialise_environment(options);

conc_dir = meta.conc_dir;
save_dir = meta.save_dir;

%% Find records with both CIF and FOB

t = 2017;
n_records = 25;

% Read raw data
fname = [comtrade_dir 'comtrade-finished/' 'comtrade-joined-records-' num2str(t) '.csv'];
assert(isfile(fname));

T = readtable(fname);

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

tmp = cell2mat(trade(:,[col_idx.value_fob col_idx.value_cif col_idx.reporter_code col_idx.partner_code]));

a = intersect(find(~isnan(tmp(:,1))), find(~isnan(tmp(:,2)))); % both CIF and FOB values
b = find(strcmp(trade(:,col_idx.flow),'M')); % reported as import
c = intersect(find(tmp(:,1) > 0), find(tmp(:,2) > 0));
d = intersect(intersect(a,b), c);

% Look for matching export